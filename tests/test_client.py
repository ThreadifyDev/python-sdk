import json
from unittest.mock import AsyncMock

import pytest

from threadify.client import Threadify
from threadify.models import ConnectOptions


class TestThreadifyConnect:
    @pytest.mark.asyncio
    async def test_requires_ws_url(self, monkeypatch):
        ws_connect = AsyncMock(
            side_effect=AssertionError("websockets.connect should not be called")
        )
        monkeypatch.setattr("threadify.client.websockets.connect", ws_connect)

        with pytest.raises(ValueError, match="ws_url is required"):
            await Threadify.connect("test-api-key", "test-service")

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
    def test_create_requires_ws_url(self):
        with pytest.raises(ValueError, match="ws_url is required"):
            Threadify.create("test-api-key")
