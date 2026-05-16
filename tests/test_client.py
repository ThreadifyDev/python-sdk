import json
from unittest.mock import AsyncMock

import pytest

from threadify.client import Threadify
from threadify.models import ConnectOptions


class TestThreadifyConnect:
    @pytest.mark.asyncio
    async def test_connect_uses_default_ws_url_when_not_provided(self, monkeypatch):
        ws = AsyncMock()
        ws.send = AsyncMock()
        ws.recv = AsyncMock(return_value=json.dumps({"action": "connect", "status": "success"}))
        ws.close = AsyncMock()

        ws_connect = AsyncMock(return_value=ws)
        monkeypatch.setattr("threadify.client.websockets.connect", ws_connect)

        conn = await Threadify.connect("test-api-key", "test-service")

        ws_connect.assert_awaited_once_with("wss://eng.threadify.dev/threads")
        sent_msg = json.loads(ws.send.call_args.args[0])
        assert sent_msg["serviceName"] == "test-service"
        assert conn.is_connected is True

    @pytest.mark.asyncio
    async def test_connect_with_explicit_ws_url(self, monkeypatch):
        ws = AsyncMock()
        ws.send = AsyncMock()
        ws.recv = AsyncMock(return_value=json.dumps({"action": "connect", "status": "success"}))
        ws.close = AsyncMock()

        ws_connect = AsyncMock(return_value=ws)
        monkeypatch.setattr("threadify.client.websockets.connect", ws_connect)

        conn = await Threadify.connect(
            "test-api-key",
            service_name="test-service",
            ws_url="wss://example.com/threads",
        )

        ws_connect.assert_awaited_once_with("wss://example.com/threads")
        sent = ws.send.call_args.args[0]
        sent_msg = json.loads(sent)
        assert sent_msg["action"] == "connect"
        assert sent_msg["apiKey"] == "test-api-key"
        assert sent_msg["serviceName"] == "test-service"
        assert conn.is_connected is True

    @pytest.mark.asyncio
    async def test_connect_legacy_signature_still_supported(self, monkeypatch):
        ws = AsyncMock()
        ws.send = AsyncMock()
        ws.recv = AsyncMock(return_value=json.dumps({"action": "connect", "status": "success"}))
        ws.close = AsyncMock()

        ws_connect = AsyncMock(return_value=ws)
        monkeypatch.setattr("threadify.client.websockets.connect", ws_connect)

        conn = await Threadify.connect(
            "test-api-key",
            "legacy-service",
            ConnectOptions(ws_url="wss://example.com/threads"),
        )

        sent_msg = json.loads(ws.send.call_args.args[0])
        assert sent_msg["serviceName"] == "legacy-service"
        assert conn.is_connected is True


class TestThreadifyFactory:
    def test_create_uses_default_ws_url(self):
        factory = Threadify.create("test-api-key")
        assert factory._options.ws_url == "wss://eng.threadify.dev/threads"
