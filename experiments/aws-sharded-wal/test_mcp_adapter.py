"""Offline completion-boundary checks for the optional official MCP proxy adapter."""
from contextlib import asynccontextmanager
import importlib.util
import unittest
from unittest.mock import AsyncMock, patch

if importlib.util.find_spec('mcp') is not None:
    import mcp_adapter
else:
    mcp_adapter = None


@unittest.skipIf(mcp_adapter is None, 'install the controller MCP SDK to exercise its adapter')
class McpCompletionTests(unittest.IsolatedAsyncioTestCase):
    async def test_completion_on_last_allowed_poll_returns_verified_result(self):
        completed = {'status': 'success', 'return_value': {'created': 'owned-resource'}}
        class Response:
            def __init__(self, value):
                self.value = value

            def model_dump(self, **kwargs):
                return {'structuredContent': self.value}

        class Session:
            def __init__(self, *args):
                self.polls = 0

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def initialize(self):
                return None

            async def call_tool(self, name, arguments):
                if name == 'aws___run_script':
                    return Response({'status': 'working', 'task_id': 'owned-task'})
                self.polls += 1
                task = {'task_id': 'owned-task', 'status': 'working'}
                if self.polls == 7:
                    task.update(status='completed', result=completed)
                return Response({'tasks': [task]})

        @asynccontextmanager
        async def transport(parameters):
            yield None, None

        with patch.object(mcp_adapter, 'stdio_client', transport), \
             patch.object(mcp_adapter, 'ClientSession', Session), \
             patch.object(mcp_adapter.asyncio, 'sleep', AsyncMock()):
            result = await mcp_adapter.tool_async('aws___run_script', {}, 'us-east-1', 'tidehunter')
        self.assertEqual(result, completed)
