#!/usr/bin/env python3
"""Run the existing synchronous budget ledger through an async, MCP-only API bridge.

The caller supplies its authenticated session-tool callback. This module has no
AWS credential chain, CLI fallback, provisioning operation or alternate ledger.
"""
import asyncio
import io
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
BUCKET = "tidehunter-ci-354775971805-us-east-1"
ACCOUNT = "354775971805"


class McpS3:
    def __init__(self, api, loop, *, timeout=120):
        self.api, self.loop, self.timeout = api, loop, timeout

    def _request(self, operation, params):
        if params.get("Bucket") != BUCKET or params.get("ExpectedBucketOwner") != ACCOUNT:
            raise ValueError("budget bridge refuses another bucket/account")
        key = params.get("Key", "")
        if not key.startswith(("ci-control/budget/", "ci-control/reports/")) or ".." in key.split("/"):
            raise ValueError("budget bridge refuses a non-ledger/report key")
        response = asyncio.run_coroutine_threadsafe(
            self.api("s3", operation, params, "us-east-1"), self.loop).result(self.timeout)
        if not isinstance(response, dict):
            raise ValueError("MCP did not return an actual S3 response")
        return response

    def get_object(self, **params):
        response = self._request("GetObject", params)
        body = response.get("Body")
        if not isinstance(body, str):
            raise ValueError("MCP budget body must preserve its UTF-8 JSON bytes")
        raw = body.encode("utf-8")
        if response.get("ContentLength") != len(raw) or not response.get("ETag"):
            raise ValueError("MCP budget body length/ETag differs from actual S3 identity")
        return {**response, "Body": io.BytesIO(raw)}

    def put_object(self, **params):
        body = params.get("Body")
        if not isinstance(body, bytes):
            raise ValueError("ledger must supply its exact serialized bytes")
        if params.get("Key", "").startswith("ci-control/budget/"):
            if bool(params.get("IfMatch")) == bool(params.get("IfNoneMatch")):
                raise ValueError("ledger mutation requires exactly one existing CAS precondition")
        # Boto3 serializes a UTF-8 string to these same bytes. No JSON parse or
        # reserialization is allowed between the ledger and the MCP operation.
        return self._request("PutObject", {**params, "Body": body.decode("utf-8")})


async def transact(api, operation, *, run_id=None, estimate_usd=None, provenance=None,
                   usage_usd=None, receipt=None, budget_path=None):
    """Call real BudgetLedger methods, including their conditional S3 writes."""
    directory = HERE.parent / "aws-sharded-wal"
    sys.path.insert(0, str(directory))
    try:
        from ci_budget import BudgetLedger
    finally:
        sys.path.pop(0)
    ledger = BudgetLedger(McpS3(api, asyncio.get_running_loop()), BUCKET, ACCOUNT,
                          budget_path=budget_path or directory / "budget.json")
    if operation == "report":
        return await asyncio.to_thread(ledger.report)
    if operation == "reserve":
        return await asyncio.to_thread(ledger.reserve, run_id, estimate_usd, provenance)
    if operation == "settle":
        return await asyncio.to_thread(ledger.settle, run_id, usage_usd, receipt)
    raise ValueError("unsupported budget transaction")
