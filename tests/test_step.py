from unittest.mock import AsyncMock, MagicMock

import pytest

from threadify.step import ThreadStep, _fnv1a_32


def _make_thread():
    """Create a mock ThreadInstance with a mock connection."""
    conn = MagicMock()
    conn.service_name = "test-service"
    conn._wait_response = AsyncMock()
    conn._send = AsyncMock()

    thread = MagicMock()
    thread.thread_id = "thread-001"
    thread._conn = conn
    thread._send = AsyncMock()

    return thread


class TestThreadStepFluent:
    def test_chaining(self):
        thread = _make_thread()
        step = ThreadStep("order_placed", thread, "test-svc")
        result = step.add_context({"orderId": "ORD-1", "amount": "99.99"}).add_refs(
            {"stripe": "pi_abc"}
        )
        assert result is step  # Fluent returns self.
        assert step.context == {"orderId": "ORD-1", "amount": "99.99"}

    def test_add_context_converts_to_strings(self):
        thread = _make_thread()
        step = ThreadStep("s", thread, "svc")
        step.add_context({"count": 42, "active": True})
        ctx = step.context
        assert ctx["count"] == "42"
        assert ctx["active"] == "True"

    def test_add_context_none(self):
        thread = _make_thread()
        step = ThreadStep("s", thread, "svc")
        step.add_context(None)
        assert step.context == {}

    def test_private_context(self):
        thread = _make_thread()
        step = ThreadStep("s", thread, "svc")
        step.add_private_context({"secret": "xyz"})
        ctx = step.context
        assert ctx["secret"] == "xyz"
        assert ctx["private_secret"] == "xyz"


class TestIdempotencyKey:
    def test_manual_key(self):
        thread = _make_thread()
        step = ThreadStep("s", thread, "svc")
        step.idempotency_key("custom-key")
        key = step._generate_idempotency_key()
        assert key == "custom-key"

    def test_auto_key_deterministic(self):
        thread = _make_thread()
        step1 = ThreadStep("order_placed", thread, "svc")
        step1.add_context({"orderId": "ORD-123", "amount": "99.99"})

        step2 = ThreadStep("order_placed", thread, "svc")
        step2.add_context({"amount": "99.99", "orderId": "ORD-123"})  # Different order.

        k1 = step1._generate_idempotency_key()
        k2 = step2._generate_idempotency_key()
        assert k1 == k2, "Same name + same context (different order) should produce same key"

    def test_auto_key_format(self):
        thread = _make_thread()
        step = ThreadStep("order_placed", thread, "svc")
        step.add_context({"foo": "bar"})
        key = step._generate_idempotency_key()
        assert len(key) == 8
        int(key, 16)  # Should be valid hex.

    def test_empty_key_sets_error(self):
        thread = _make_thread()
        step = ThreadStep("s", thread, "svc")
        returned = step.idempotency_key("")
        assert returned is step
        assert step._error is not None

    def test_whitespace_key_sets_error(self):
        thread = _make_thread()
        step = ThreadStep("s", thread, "svc")
        returned = step.idempotency_key("   ")
        assert returned is step
        assert step._error is not None


class TestFNV1a:
    def test_known_values(self):
        # FNV-1a 32-bit for empty string.
        assert _fnv1a_32(b"") == 0x811C9DC5

    def test_deterministic(self):
        assert _fnv1a_32(b"hello") == _fnv1a_32(b"hello")

    def test_different_inputs(self):
        assert _fnv1a_32(b"a") != _fnv1a_32(b"b")


class TestSubSteps:
    def test_sub_step_success(self):
        thread = _make_thread()
        step = ThreadStep("main", thread, "svc")
        step.sub_step("sub1", {"data": "val"}, "success")
        assert len(step._sub_steps) == 1
        assert step._sub_steps[0].name == "sub1"
        assert step._sub_steps[0].status == "success"

    def test_sub_step_failed(self):
        thread = _make_thread()
        step = ThreadStep("main", thread, "svc")
        step.sub_step("sub1", status="failed")
        assert step._sub_steps[0].status == "failed"

    def test_sub_step_invalid_status(self):
        thread = _make_thread()
        step = ThreadStep("main", thread, "svc")
        returned = step.sub_step("sub1", status="invalid")
        assert returned is step
        assert step._error is not None

    def test_sub_step_empty_name(self):
        thread = _make_thread()
        step = ThreadStep("main", thread, "svc")
        returned = step.sub_step("")
        assert returned is step
        assert step._error is not None


class TestStepStatusMethods:
    @pytest.mark.asyncio
    async def test_success(self):
        thread = _make_thread()
        thread._conn._wait_response.return_value = {
            "action": "recordThreadEvent",
            "status": "success",
        }

        step = ThreadStep("order_placed", thread, "svc")
        step.add_context({"orderId": "ORD-1"})
        result = await step.success("Order received")

        assert result.step_name == "order_placed"
        assert result.status == "success"
        assert result.thread_id == "thread-001"
        assert result.duplicate is False
        assert len(result.idempotency_key) == 8

    @pytest.mark.asyncio
    async def test_failed(self):
        thread = _make_thread()
        thread._conn._wait_response.return_value = {
            "action": "recordThreadEvent",
            "status": "success",
        }

        step = ThreadStep("payment", thread, "svc")
        result = await step.failed("Insufficient funds")

        assert result.status == "failed"

    @pytest.mark.asyncio
    async def test_error(self):
        thread = _make_thread()
        thread._conn._wait_response.return_value = {
            "action": "recordThreadEvent",
            "status": "success",
        }

        step = ThreadStep("api_call", thread, "svc")
        result = await step.error("Timeout")

        assert result.status == "error"

    @pytest.mark.asyncio
    async def test_success_with_map_data(self):
        thread = _make_thread()
        thread._conn._wait_response.return_value = {
            "action": "recordThreadEvent",
            "status": "success",
        }

        step = ThreadStep("step1", thread, "svc")
        result = await step.success({"key": "value", "count": 42})

        assert result.status == "success"

    @pytest.mark.asyncio
    async def test_duplicate_detection(self):
        thread = _make_thread()
        thread._conn._wait_response.return_value = {
            "action": "recordThreadEvent",
            "status": "error",
            "message": "duplicate step",
            "isDuplicate": True,
        }

        step = ThreadStep("order_placed", thread, "svc")
        result = await step.success()

        assert result.duplicate is True

    @pytest.mark.asyncio
    async def test_server_error(self):
        thread = _make_thread()
        thread._conn._wait_response.return_value = {
            "action": "recordThreadEvent",
            "status": "error",
            "message": "internal server error",
        }

        step = ThreadStep("broken", thread, "svc")
        with pytest.raises(RuntimeError, match="internal server error"):
            await step.error()

    @pytest.mark.asyncio
    async def test_invalid_sub_step_status_raised_on_stop(self):
        thread = _make_thread()
        step = ThreadStep("order_placed", thread, "svc")
        step.sub_step("sub", status="invalid")

        with pytest.raises(ValueError, match="sub-step status"):
            await step.success()

    @pytest.mark.asyncio
    async def test_invalid_idempotency_key_raised_on_stop(self):
        thread = _make_thread()
        step = ThreadStep("order_placed", thread, "svc")
        step.idempotency_key("")

        with pytest.raises(ValueError, match="idempotency key"):
            await step.success()

    @pytest.mark.asyncio
    async def test_empty_step_name(self):
        # Use the real thread.step() which now defers validation.
        real_conn = MagicMock()
        real_conn.service_name = "svc"
        from threadify.thread import ThreadInstance

        real_thread = ThreadInstance(real_conn, "t1")

        step = real_thread.step("")
        assert step is not None
        assert step._error is not None

        with pytest.raises(ValueError, match="step_name"):
            await step.success()


class TestStepProperties:
    def test_step_name(self):
        thread = _make_thread()
        step = ThreadStep("order_placed", thread, "svc")
        assert step.step_name == "order_placed"

    def test_initial_status(self):
        thread = _make_thread()
        step = ThreadStep("order_placed", thread, "svc")
        assert step.status == "in_progress"

    def test_get_event_data(self):
        thread = _make_thread()
        step = ThreadStep("order_placed", thread, "svc")
        step.add_context({"a": "b"})
        data = step.get_event_data()
        assert data["action"] == "recordThreadEvent"
        assert data["stepName"] == "order_placed"
        assert data["threadId"] == "thread-001"
