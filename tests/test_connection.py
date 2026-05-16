import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from threadify.connection import (
    Connection,
    _build_event_types,
    _merge_unique,
    _parse_event,
)


def _make_mock_ws():
    """Create a mock WebSocket object."""
    ws = AsyncMock()
    ws.send = AsyncMock()
    ws.close = AsyncMock()
    # Mock async iteration (empty by default).
    ws.__aiter__ = MagicMock(return_value=iter([]))
    return ws


def _make_connection():
    """Create a Connection with mocked WebSocket (listener disabled)."""
    ws = _make_mock_ws()
    conn = Connection.__new__(Connection)
    conn._ws = ws
    conn._api_key = "test-key"
    conn._service_name = "test-service"
    conn._graphql_url = "https://example.com/graphql"
    conn._debug = False
    conn._max_in_flight = 10
    conn._connected = True
    conn._threads = {}
    conn._notification_handlers = {}
    conn._active_subscriptions = {}
    conn._processed_notifications = set()
    conn._processed_notifications_max_size = 10_000
    conn._recv_queue = asyncio.Queue()
    conn._data_retriever = None
    import logging

    conn._logger = logging.getLogger("threadify-test")
    # Create a completed listener task.
    loop = asyncio.get_event_loop()
    conn._listener_task = loop.create_future()
    conn._listener_task.set_result(None)
    return conn


class TestParseEvent:
    @pytest.mark.parametrize(
        "event,expected_source,expected_type",
        [
            ("step.success", "execution", "success"),
            ("step.failed", "execution", "failed"),
            ("rule.violated", "validation", "violated"),
            ("rule.passed", "validation", "passed"),
            ("step.*", "execution", "*"),
            ("rule.*", "validation", "*"),
            ("*", "*", "*"),
        ],
    )
    def test_parse(self, event, expected_source, expected_type):
        source, etype = _parse_event(event)
        assert source == expected_source
        assert etype == expected_type


class TestBuildEventTypes:
    def test_wildcard_all(self):
        result = _build_event_types("*", "*")
        assert set(result) == {
            "execution.success",
            "execution.failed",
            "validation.passed",
            "validation.violated",
        }

    def test_execution_wildcard(self):
        result = _build_event_types("execution", "*")
        assert set(result) == {"execution.success", "execution.failed"}

    def test_validation_wildcard(self):
        result = _build_event_types("validation", "*")
        assert set(result) == {"validation.passed", "validation.violated"}

    def test_specific(self):
        result = _build_event_types("execution", "success")
        assert result == ["execution.success"]


class TestMergeUnique:
    def test_no_overlap(self):
        result = _merge_unique(["a", "b"], ["c", "d"])
        assert len(result) == 4

    def test_full_overlap(self):
        result = _merge_unique(["a", "b"], ["a", "b"])
        assert len(result) == 2

    def test_partial_overlap(self):
        result = _merge_unique(["a", "b"], ["b", "c"])
        assert len(result) == 3

    def test_empty(self):
        result = _merge_unique([], [])
        assert len(result) == 0


class TestConnectionProperties:
    @pytest.mark.asyncio
    async def test_service_name(self):
        conn = _make_connection()
        assert conn.service_name == "test-service"

    @pytest.mark.asyncio
    async def test_is_connected(self):
        conn = _make_connection()
        assert conn.is_connected is True


class TestStart:
    @pytest.mark.asyncio
    async def test_start_with_label_and_optional_contract(self):
        conn = _make_connection()
        conn._send = AsyncMock()
        conn._wait_response = AsyncMock(
            return_value={
                "action": "startThread",
                "status": "success",
                "threadId": "thread-123",
            }
        )

        thread = await conn.start("customer-123", "customer")

        assert thread.thread_id == "thread-123"
        assert thread.role == "test"
        assert thread.refs["label"] == "customer-123"
        assert thread.refs["serviceName"] == "test-service"
        sent_msg = conn._send.call_args.args[0]
        assert sent_msg["refs"]["label"] == "customer-123"
        assert sent_msg["contractName"] == "customer"
        assert sent_msg["role"] == "test"

    @pytest.mark.asyncio
    async def test_start_with_legacy_refs_first_signature(self):
        conn = _make_connection()
        conn._send = AsyncMock()
        conn._wait_response = AsyncMock(
            return_value={
                "action": "startThread",
                "status": "success",
                "threadId": "thread-legacy-123",
            }
        )

        thread = await conn.start({"customer_id": "123"}, "customer")

        assert thread.thread_id == "thread-legacy-123"
        assert thread.refs["customer_id"] == "123"
        assert thread.refs["serviceName"] == "test-service"
        sent_msg = conn._send.call_args.args[0]
        assert sent_msg["refs"]["customer_id"] == "123"
        assert "label" not in sent_msg["refs"]
        assert sent_msg["contractName"] == "customer"

    @pytest.mark.asyncio
    async def test_start_without_contract_still_works(self):
        conn = _make_connection()
        conn._send = AsyncMock()
        conn._wait_response = AsyncMock(
            return_value={
                "action": "startThread",
                "status": "success",
                "threadId": "thread-no-contract-123",
            }
        )

        thread = await conn.start("customer-123")

        assert thread.thread_id == "thread-no-contract-123"
        assert thread.refs["label"] == "customer-123"
        assert thread.refs["serviceName"] == "test-service"
        sent_msg = conn._send.call_args.args[0]
        assert sent_msg["refs"]["label"] == "customer-123"
        assert "contractName" not in sent_msg
        assert "role" not in sent_msg

    @pytest.mark.asyncio
    async def test_start_accepts_keyword_config_options(self):
        conn = _make_connection()
        conn._send = AsyncMock()
        conn._wait_response = AsyncMock(
            return_value={
                "action": "startThread",
                "status": "success",
                "threadId": "thread-options-123",
            }
        )

        thread = await conn.start(
            contract_name="order_processing",
            role="merchant",
            refs={"customer_id": "123"},
            tags=["priority", "external"],
        )

        assert thread.thread_id == "thread-options-123"
        assert thread.role == "merchant"
        assert thread.tags == ["priority", "external"]
        assert thread.refs["customer_id"] == "123"
        assert thread.refs["serviceName"] == "test-service"
        sent_msg = conn._send.call_args.args[0]
        assert sent_msg["contractName"] == "order_processing"
        assert sent_msg["role"] == "merchant"
        assert sent_msg["refs"]["customer_id"] == "123"
        assert sent_msg["tags"] == ["priority", "external"]


class TestJoin:
    @pytest.mark.asyncio
    async def test_join_with_token_keyword(self):
        conn = _make_connection()
        conn._send = AsyncMock()
        conn._wait_response = AsyncMock(
            return_value={
                "action": "joinThread",
                "status": "success",
                "threadId": "thread-token-123",
                "role": "logistics",
            }
        )

        thread = await conn.join(token="short-token")

        assert thread.thread_id == "thread-token-123"
        sent_msg = conn._send.call_args.args[0]
        assert sent_msg["threadToken"] == "short-token"
        assert "threadId" not in sent_msg

    @pytest.mark.asyncio
    async def test_join_with_thread_id_and_role_keywords(self):
        conn = _make_connection()
        conn._send = AsyncMock()
        conn._wait_response = AsyncMock(
            return_value={
                "action": "joinThread",
                "status": "success",
                "threadId": "thread-direct-123",
                "role": "logistics",
            }
        )

        thread = await conn.join(thread_id="thread-direct-123", role="logistics")

        assert thread.thread_id == "thread-direct-123"
        sent_msg = conn._send.call_args.args[0]
        assert sent_msg["threadId"] == "thread-direct-123"
        assert sent_msg["role"] == "logistics"
        assert "threadToken" not in sent_msg

    @pytest.mark.asyncio
    async def test_join_requires_input(self):
        conn = _make_connection()
        with pytest.raises(ValueError, match="provide"):
            await conn.join()

    @pytest.mark.asyncio
    async def test_join_legacy_signature_still_supported(self):
        conn = _make_connection()
        conn._send = AsyncMock()
        conn._wait_response = AsyncMock(
            return_value={
                "action": "joinThread",
                "status": "success",
                "threadId": "legacy-thread-123",
                "role": "operator",
            }
        )

        thread = await conn.join("legacy-thread-123", role="operator")
        assert thread.thread_id == "legacy-thread-123"
        sent_msg = conn._send.call_args.args[0]
        assert sent_msg["threadId"] == "legacy-thread-123"
        assert sent_msg["role"] == "operator"


class TestNotificationHandling:
    @pytest.mark.asyncio
    async def test_subscribe_unsubscribe(self):
        conn = _make_connection()

        called = False

        def handler(n):
            nonlocal called
            called = True

        conn.subscribe("step.success", "order_placed", handler)
        assert "step.success:order_placed" in conn._notification_handlers

        conn.unsubscribe("step.success", "order_placed")
        assert "step.success:order_placed" not in conn._notification_handlers

    @pytest.mark.asyncio
    async def test_subscribe_none_handler(self):
        conn = _make_connection()
        with pytest.raises(ValueError, match="handler"):
            conn.subscribe("step.success", "x", None)
        assert "step.success:x" not in conn._notification_handlers

    @pytest.mark.asyncio
    async def test_deduplication(self):
        conn = _make_connection()

        call_count = 0

        def handler(n):
            nonlocal call_count
            call_count += 1

        conn.subscribe("step.success", "order_placed", handler)

        notif_data = {
            "notificationId": "n-001",
            "threadId": "t-1",
            "stepName": "order_placed",
            "source": "execution",
            "notificationType": "execution.success",
            "stepStatus": "success",
        }

        conn._handle_notification(notif_data, "ack-1")
        conn._handle_notification(notif_data, "ack-1")

        assert call_count == 1, "Handler should be called once (deduplicated)"

    @pytest.mark.asyncio
    async def test_empty_notification_ignored(self):
        conn = _make_connection()
        conn._handle_notification({}, "")
        conn._handle_notification(None, "")
        # Should not raise.

    @pytest.mark.asyncio
    async def test_handler_exception_caught(self):
        conn = _make_connection()
        conn._debug = True

        def bad_handler(n):
            raise RuntimeError("boom")

        conn.subscribe("step.failed", "broken_step", bad_handler)

        notif_data = {
            "notificationId": "n-err",
            "threadId": "t-1",
            "stepName": "broken_step",
            "source": "execution",
            "notificationType": "execution.failed",
            "stepStatus": "failed",
        }

        # Should not raise even though handler throws.
        conn._handle_notification(notif_data, "")


class TestConnectionClose:
    @pytest.mark.asyncio
    async def test_close(self):
        conn = _make_connection()
        await conn.close()
        assert conn.is_connected is False
        conn._ws.close.assert_called_once()
