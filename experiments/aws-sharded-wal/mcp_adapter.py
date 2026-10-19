"""AWS API adapter over the installed official MCP proxy; no CLI/API fallback.

Retains only the MCP tool/API bridge from the previous fleet adapter.
Uses the MCP SDK 2.x exception contract. No AWS CLI or local SDK
operation is issued; object transfers use MCP-issued HTTPS URLs.
"""
import argparse
import asyncio
import http.client
import io
import json
import os
from pathlib import Path
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

import boto3
import botocore.session
from botocore import xform_name
from botocore.exceptions import ClientError, EndpointConnectionError, WaiterError
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.shared.exceptions import MCPError


def unpack(response):
    document = response.model_dump(mode="json", by_alias=True)
    value = document.get("structuredContent") or document.get("structured_content")
    if value is None:
        texts = [item["text"] for item in document.get("content", []) if item.get("type") == "text"]
        if len(texts) != 1:
            raise RuntimeError("AWS MCP response has no unambiguous structured result")
        value = json.loads(texts[0])
    if document.get("isError", document.get("is_error", False)):
        raise RuntimeError("AWS MCP tool error: " + json.dumps(value))
    return value


async def tool_async(name, arguments, region, profile):
    parameters = StdioServerParameters(command="uvx", args=[
        "mcp-proxy-for-aws==1.7.0", "https://aws-mcp.us-east-1.api.aws/mcp",
        "--region", "us-east-1", "--profile", profile, "--connect-timeout", "30",
        "--read-timeout", "90", "--tool-timeout", "95",
        "--metadata", f"AWS_REGION={region}", "INSTALL_SOURCE=aws-cli"],
        env=os.environ | {"UV_OFFLINE": "1", "AWS_REGION": region, "AWS_DEFAULT_REGION": region})
    async with asyncio.timeout(150):
        async with stdio_client(parameters) as (reader, writer):
            async with ClientSession(reader, writer) as session:
                await session.initialize()
                result = unpack(await session.call_tool(name, arguments))
                for iteration in range(1, 8):
                    if result.get("status") != "working":
                        return result
                    task_id = result.get("task_id")
                    if not task_id:
                        raise RuntimeError("AWS MCP pending response omitted its task ID")
                    await asyncio.sleep(min(5, result.get("recommended_wait_seconds", 2)))
                    result = unpack(await session.call_tool("aws___get_tasks", {
                        "task_ids": [task_id], "poll_iteration": iteration}))
                    tasks = result.get("tasks")
                    if not isinstance(tasks, list) or len(tasks) != 1 or tasks[0].get("task_id") != task_id:
                        raise ConnectionError("AWS MCP task response did not match the pending operation")
                    task = tasks[0]
                    if task.get("status") in ("working", "pending", "running"):
                        result = task | {"status": "working"}
                    elif isinstance(task.get("result"), dict):
                        return task["result"]
                    else:
                        raise ConnectionError("AWS MCP task did not return a verifiable completed result")
                raise TimeoutError("AWS MCP task polling limit; operation outcome unknown")


def tool(name, arguments, region="us-east-1", profile="tidehunter"):
    try:
        return asyncio.run(tool_async(name, arguments, region, profile))
    except (OSError, EOFError, ExceptionGroup, MCPError) as error:
        # A failed transport does not establish whether a mutation ran. Preserve
        # the SDK retry category and the CLI controller's uncertain-outcome path.
        raise EndpointConnectionError(endpoint_url="https://aws-mcp.us-east-1.api.aws/mcp") from error


def api(service, operation, params, region="us-east-1", profile="tidehunter"):
    arguments = {"service_name": service, "operation_name": operation,
                 "region_name": region, "params": params}
    # JSON keeps policy documents and user data literal within the Python script.
    code = ("request = json.loads(" + repr(json.dumps(arguments)) + ")\n"
            "try:\n"
            "    value = await call_boto3(**request)\n"
            "    result = {\"value\": value}\n"
            # The live MCP sandbox does not define the ClientError name advertised
            # by its schema. Its API exceptions still expose the SDK response.
            "except Exception as error:\n"
            "    result = {\"error\": error.response}\n"
            "result")
    response = tool("aws___run_script", {"code": code}, region, profile)
    if response.get("status") != "success":
        raise EndpointConnectionError(endpoint_url="AWS MCP script result unavailable")
    calls = response.get("api_calls", [])
    if not any(call.get("service") == service and call.get("operation") == operation for call in calls):
        raise EndpointConnectionError(endpoint_url="AWS MCP did not record the requested API call")
    result = response["return_value"]
    if "error" in result:
        raise ClientError(result["error"], operation)
    return result["value"]

