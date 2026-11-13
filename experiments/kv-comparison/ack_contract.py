"""External-store acknowledgement contracts; absent settings preserve strict runs."""

# Kvrocks page_cache uses RocksDB non-sync writes, not a userspace WAL buffer:
# https://github.com/meta/rocksdb/wiki/Basic-Operations#non-sync-writes
# Pinned Kvrocks storage.cc:116-121 sets sync/disableWAL; InitRocksDBOptions
# (:169-243) leaves manual_wal_flush at its default; :753 returns db_->Write status.
# Pinned RocksDB v11.8.1 options.h:1569 defaults manual_wal_flush to false.
# https://github.com/apache/kvrocks/blob/185d4221df20af8ed3416e4dec4de8917312a2e9/src/storage/storage.cc
# https://github.com/facebook/rocksdb/blob/v11.8.1/include/rocksdb/options.h#L1569
# Valkey non-ALWAYS write failures and the later event-loop I/O step:
# https://github.com/valkey-io/valkey/blob/fe8d406c0b5a64ec897f6455e4de684629883764/src/aof.c#L1194-L1218
# https://github.com/valkey-io/valkey/blob/fe8d406c0b5a64ec897f6455e4de684629883764/src/server.c#L1898-L1928


def selected(backend, value="synced"):
    if value not in ("synced", "page_cache", "periodic_sync", "os_buffered"):
        raise ValueError("ack_contract must be synced, page_cache, os_buffered or periodic_sync")
    supported = ("kvrocks",) if value == "page_cache" else ("valkey_tcp", "valkey_rdma")
    if value != "synced" and backend not in supported:
        raise ValueError(f"{value} ack_contract is not supported for {backend}")
    return value


def valkey_fsync(value="synced"):
    selected("valkey_tcp", value)
    return {"synced": "always", "os_buffered": "no", "periodic_sync": "everysec"}[value]


def acknowledgement(backend, value="synced"):
    """Everysec can defer writes: https://valkey.io/topics/latency/#aof-and-disk-io."""
    selected(backend, value)
    if value == "synced":
        raise ValueError("synced acknowledgement text comes from the observed backend receipt")
    if value == "os_buffered":
        return "AOF write issued to OS in event loop before queued replies in normal path; failed/short write not fatal and replies still go out; replies from later I/O step same loop not covered; Valkey issues no fsync"
    if value == "periodic_sync":
        return "AOF enabled; reply after the AOF buffer write is scheduled; fsync once per second (appendfsync everysec); writes can remain in user space for up to two seconds during fsync; neither OS-page-cache-before-reply nor synced-before-acknowledgement is guaranteed"
    return "RocksDB WAL enabled; write_options.sync=no; acknowledgement without WAL fsync (OS page cache); no power-loss durability guarantee"


