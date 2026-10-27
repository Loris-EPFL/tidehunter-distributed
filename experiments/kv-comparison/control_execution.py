"""Durable local at-most-once execution gate for guest operations.

A started record is permanent evidence that the callback may have taken effect.
It is deliberately not recovered by rerunning the callback after a crash.
"""
from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import stat
import time
from typing import Any, Callable


MAX_DEADLINE_EPOCH = 253_402_300_799.0

MAX_JSON_BYTES = 32 * 1024 * 1024
MAX_RECORD_BYTES = 2 * MAX_JSON_BYTES + 8 * 1024
MAX_KEY_BYTES = 1024
MAX_JSON_NODES = MAX_RECORD_BYTES
MAX_JSON_DEPTH = 64
MAX_LOCK_WAIT_SECONDS = 10_800.0
LOCK_POLL_SECONDS = 0.01
MAX_LOCK_ATTEMPTS = int(MAX_LOCK_WAIT_SECONDS / LOCK_POLL_SECONDS) + 1

_GATE_DIRECTORY = ".guest-operation-gates"
_SCHEMA_VERSION = 1

JSONValue = Any


class ExecutionGateError(RuntimeError):
    """The gate cannot safely establish a reusable operation result."""


def _validate_json_value(value: Any, max_bytes: int) -> None:
    pending = [(iter((value,)), 0, False)]
    nodes = 0
    string_chars = 0
    while pending:
        iterator, depth, mapping_items = pending[-1]
        try:
            item = next(iterator)
        except StopIteration:
            pending.pop()
            continue
        if mapping_items:
            key, item = item
            if type(key) is not str:
                raise ExecutionGateError("JSON object keys must be strings")
            string_chars += len(key)
            if string_chars > max_bytes:
                raise ExecutionGateError("JSON strings exceed the size limit")

        nodes += 1
        if nodes > MAX_JSON_NODES:
            raise ExecutionGateError("JSON value exceeds the node limit")
        if depth > MAX_JSON_DEPTH:
            raise ExecutionGateError("JSON value exceeds the nesting limit")
        kind = type(item)
        if item is None or kind is bool:
            continue
        if kind is str:
            string_chars += len(item)
            if string_chars > max_bytes:
                raise ExecutionGateError("JSON strings exceed the size limit")
            continue
        if kind is int:
            if item.bit_length() > max_bytes * 4:
                raise ExecutionGateError("JSON integer exceeds the size limit")
            continue
        if kind is float:
            if not math.isfinite(item):
                raise ExecutionGateError("JSON numbers must be finite")
            continue
        if kind is list:
            if len(item) + nodes > MAX_JSON_NODES:
                raise ExecutionGateError("JSON value exceeds the node limit")
            pending.append((iter(item), depth + 1, False))
            continue
        if kind is dict:
            if len(item) + nodes > MAX_JSON_NODES:
                raise ExecutionGateError("JSON value exceeds the node limit")
            pending.append((iter(item.items()), depth + 1, True))
            continue
        raise ExecutionGateError(f"unsupported JSON value type: {kind.__name__}")


def _canonical_json(value: Any, *, max_bytes: int = MAX_JSON_BYTES) -> bytes:
    _validate_json_value(value, max_bytes)
    encoder = json.JSONEncoder(
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    chunks = []
    size = 0
    try:
        for chunk in encoder.iterencode(value):
            encoded = chunk.encode("ascii")
            size += len(encoded)
            if size > max_bytes:
                raise ExecutionGateError("canonical JSON exceeds the size limit")
            chunks.append(encoded)
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ExecutionGateError("value cannot be encoded as bounded canonical JSON") from exc
    return b"".join(chunks)


def _normalize_key(operation_key: str) -> str:
    if type(operation_key) is not str or not operation_key:
        raise ExecutionGateError("operation_key must be a non-empty string")
    try:
        encoded = operation_key.encode("utf-8", "strict")
    except UnicodeEncodeError as exc:
        raise ExecutionGateError("operation_key must be valid UTF-8") from exc
    if len(encoded) > MAX_KEY_BYTES:
        raise ExecutionGateError("operation_key exceeds the size limit")
    return operation_key


def _normalize_deadline(deadline_epoch: float) -> float:
    if type(deadline_epoch) not in (int, float):
        raise ExecutionGateError("deadline_epoch must be a bounded finite number")
    try:
        deadline = float(deadline_epoch)
    except (OverflowError, ValueError) as exc:
        raise ExecutionGateError("deadline_epoch must be a bounded finite number") from exc
    if not math.isfinite(deadline) or not 0 <= deadline <= MAX_DEADLINE_EPOCH:
        raise ExecutionGateError("deadline_epoch must be a bounded finite number")
    return deadline


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _ensure_gate_directory(root: Path) -> Path:
    try:
        root_stat = root.lstat()
    except OSError as exc:
        raise ExecutionGateError("run-owned gate root must already exist") from exc
    if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
        raise ExecutionGateError("run-owned gate root must be a real directory")
    directory = root / _GATE_DIRECTORY

    try:
        directory.mkdir(mode=0o700)
    except FileExistsError:
        pass
    except OSError as exc:
        raise ExecutionGateError("cannot create operation gate directory") from exc

    try:
        directory_stat = directory.lstat()
    except OSError as exc:
        raise ExecutionGateError("cannot inspect operation gate directory") from exc
    if stat.S_ISLNK(directory_stat.st_mode) or not stat.S_ISDIR(directory_stat.st_mode):
        raise ExecutionGateError("operation gate path is not a real directory")
    # Every concurrent first caller synchronizes this entry before writing any
    # operation marker; the caller that performed mkdir may not be scheduled first.
    _fsync_directory(root)
    return directory


def _acquire_lock(fd: int, deadline: float) -> None:
    remaining = max(0.0, deadline - time.time())
    end = time.monotonic() + min(MAX_LOCK_WAIT_SECONDS, remaining)
    attempts = 0
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EAGAIN):
                raise ExecutionGateError("cannot acquire operation gate lock") from exc
            attempts += 1
            remaining = min(end - time.monotonic(), deadline - time.time())
            if remaining <= 0 or attempts >= MAX_LOCK_ATTEMPTS:
                raise ExecutionGateError("operation gate lock deadline expired") from exc
            time.sleep(min(LOCK_POLL_SECONDS, remaining))


def _record_bytes(fields: dict[str, Any]) -> bytes:
    checksum = hashlib.sha256(_canonical_json(fields, max_bytes=MAX_RECORD_BYTES)).hexdigest()
    record = dict(fields)
    record["record_sha256"] = checksum
    return _canonical_json(record, max_bytes=MAX_RECORD_BYTES)


def _atomic_write(path: Path, contents: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    fd = -1
    renamed = False
    try:
        fd = os.open(temporary, flags, 0o600)
        view = memoryview(contents)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short write while persisting operation record")
            view = view[written:]
        os.fsync(fd)
        os.close(fd)
        fd = -1
        os.replace(temporary, path)
        renamed = True
        _fsync_directory(path.parent)
    finally:
        if fd >= 0:
            os.close(fd)
        if not renamed:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


def _read_state(path: Path) -> dict[str, Any] | None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ExecutionGateError("cannot safely open operation state") from exc

    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > MAX_RECORD_BYTES:
            raise ExecutionGateError("operation state is not a bounded regular file")
        data = bytearray()
        while len(data) <= MAX_RECORD_BYTES:
            chunk = os.read(fd, min(65536, MAX_RECORD_BYTES + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
        if len(data) > MAX_RECORD_BYTES:
            raise ExecutionGateError("operation state exceeds the size limit")
    finally:
        os.close(fd)

    try:
        record = json.loads(bytes(data), parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    except (UnicodeDecodeError, ValueError, TypeError, RecursionError) as exc:
        raise ExecutionGateError("operation state is malformed JSON") from exc
    if type(record) is not dict:
        raise ExecutionGateError("operation state must be a JSON object")
    if _canonical_json(record, max_bytes=MAX_RECORD_BYTES) != bytes(data):
        raise ExecutionGateError("operation state is not canonical JSON")

    checksum = record.get("record_sha256")
    if type(checksum) is not str or len(checksum) != 64:
        raise ExecutionGateError("operation state checksum is malformed")
    fields = dict(record)
    del fields["record_sha256"]
    expected_checksum = hashlib.sha256(_canonical_json(fields, max_bytes=MAX_RECORD_BYTES)).hexdigest()
    if not secrets.compare_digest(checksum, expected_checksum):
        raise ExecutionGateError("operation state checksum does not match")
    return record


def _validate_record(record: dict[str, Any], key: str, request_json: bytes, deadline: float) -> Any:
    state = record.get("state")
    expected_fields = {"schema_version", "operation_key", "request", "deadline_epoch", "state", "record_sha256"}
    if state == "completed":
        expected_fields.add("result")
    if set(record) != expected_fields:
        raise ExecutionGateError("operation state has unexpected or missing fields")
    if type(record["schema_version"]) is not int or record["schema_version"] != _SCHEMA_VERSION:
        raise ExecutionGateError("operation state schema is unsupported")
    try:
        stored_key = _normalize_key(record["operation_key"])
    except (KeyError, ExecutionGateError) as exc:
        raise ExecutionGateError("operation state key is malformed") from exc
    stored_request_json = _canonical_json(record["request"])
    stored_deadline = record["deadline_epoch"]
    if type(stored_deadline) is not float or not math.isfinite(stored_deadline):
        raise ExecutionGateError("operation state deadline is malformed")
    if stored_key != key or stored_request_json != request_json or stored_deadline.hex() != deadline.hex():
        raise ExecutionGateError("operation key was reused with a changed request or deadline")
    if state == "started":
        return None
    if state != "completed":
        raise ExecutionGateError("operation state has an unknown state")
    result = record["result"]
    _canonical_json(result)
    return result


def execute_at_most_once(
    root: Path,
    operation_key: str,
    request: dict,
    execute: Callable[[], JSONValue],
    *,
    deadline_epoch: float,
) -> JSONValue:
    """Execute one JSON operation at most once and durably cache its result.

    ``root`` must be an existing, run-owned directory. If the process dies or
    ``execute`` raises after the started marker is durable, later deliveries
    fail closed rather than guessing whether the operation took effect.
    """
    key = _normalize_key(operation_key)
    if type(request) is not dict:
        raise ExecutionGateError("request must be a JSON object")
    request_json = _canonical_json(request)
    request_snapshot = json.loads(request_json)
    deadline = _normalize_deadline(deadline_epoch)
    if not callable(execute):
        raise ExecutionGateError("execute must be callable")

    run_root = Path(root).absolute()
    directory = _ensure_gate_directory(run_root)
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    state_path = directory / f"{digest}.json"
    lock_path = directory / f"{digest}.lock"
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        lock_fd = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise ExecutionGateError("cannot safely open operation gate lock") from exc

    try:
        if not stat.S_ISREG(os.fstat(lock_fd).st_mode):
            raise ExecutionGateError("operation gate lock is not a regular file")
        _acquire_lock(lock_fd, deadline)
        record = _read_state(state_path)
        if record is not None:
            result = _validate_record(record, key, request_json, deadline)
            if record["state"] == "started":
                raise ExecutionGateError("operation has an abandoned or incomplete started marker")
            return result

        if time.time() >= deadline:
            raise ExecutionGateError("expired unseen operation cannot be started")

        common = {
            "schema_version": _SCHEMA_VERSION,
            "operation_key": key,
            "request": request_snapshot,
            "deadline_epoch": deadline,
        }
        started = {**common, "state": "started"}
        _atomic_write(state_path, _record_bytes(started))

        result = execute()
        result_json = _canonical_json(result)
        result_snapshot = json.loads(result_json)
        completed = {**common, "state": "completed", "result": result_snapshot}
        _atomic_write(state_path, _record_bytes(completed))
        return result_snapshot
    finally:
        os.close(lock_fd)
