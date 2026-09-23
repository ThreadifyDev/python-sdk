import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from threadify import ConnectOptions, ThreadifyError, WaitOptions
from threadify.connection import Connection
from threadify.models import engine_endpoints, reference_query
from threadify.thread import ThreadInstance


class Socket:
    def __init__(self):
        self.incoming = asyncio.Queue()
        self.sent = asyncio.Queue()

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self.incoming.get()
        if item is None:
            raise StopAsyncIteration
        return json.dumps(item)

    async def send(self, value):
        await self.sent.put(json.loads(value))

    async def close(self):
        await self.incoming.put(None)

    async def reply(self, request, **result):
        await self.incoming.put(
            {
                "status": "success",
                "action": request["action"],
                "requestId": request["requestId"],
                "threadId": request.get("threadId"),
                "stepName": request.get("stepName"),
                **result,
            }
        )


def connected():
    ws = Socket()
    conn = Connection(ws, "test-key", "support", "http://unused/graphql")
    thread = ThreadInstance(conn, "thread-1")
    return ws, conn, thread


async def cleanup(ws, conn):
    await ws.close()
    await conn._listener_task
    conn._heartbeat_task.cancel()
    await asyncio.gather(conn._heartbeat_task, return_exceptions=True)


@pytest.mark.parametrize(
    "base,expected",
    [
        (
            "https://example.com/proxy/threads/customer/",
            (
                "wss://example.com/proxy/threads/customer/threads",
                "https://example.com/proxy/threads/customer/graphql",
            ),
        ),
        ("http://127.0.0.1:8083", ("ws://127.0.0.1:8083/threads", "http://127.0.0.1:8083/graphql")),
    ],
)
def test_engine_url_preserves_prefix(base, expected):
    assert engine_endpoints(base) == expected
    cfg = ConnectOptions(engine_url=base).with_defaults()
    assert (cfg.ws_url, cfg.graphql_url) == expected
    assert cfg.with_defaults().graphql_url == expected[1]


@pytest.mark.parametrize(
    "url",
    [
        "ftp://host",
        "relative",
        "http://user:secret@host",
        "https://host?",
        "https://host#",
        "http://host:abc",
        "http://bad host",
    ],
)
def test_invalid_engine_url(url):
    with pytest.raises(ValueError):
        engine_endpoints(url)


def test_ref_map_and_filters():
    result = reference_query({"order_id": "ORD-1001"}, status="active", limit=2)
    assert (result.ref_key, result.ref_value, result.status, result.limit) == (
        "order_id",
        "ORD-1001",
        "active",
        2,
    )
    for refs in [{}, {"a": "b", "c": "d"}, {"a": 12}, {"": "b"}]:
        with pytest.raises(ValueError):
            reference_query(refs)


@pytest.mark.asyncio
async def test_permission_report_and_exact_validation_preserve_times():
    ws, conn, thread = connected()
    try:
        waiting = asyncio.create_task(thread.wait_for("charge"))
        req = await ws.sent.get()
        assert req["await"] and req["timeoutMs"] == 10000
        # Neither a different request nor an uncorrelated legacy response can grant permission.
        await ws.incoming.put({"action": "waitFor", "status": "success", "decision": "allowed"})
        await ws.reply({**req, "requestId": "wrong"}, decision="allowed")
        await asyncio.sleep(0)
        assert not waiting.done()
        await ws.reply(req, decision="allowed", invocationId=req["invocationId"])
        grant = await waiting
        step = thread.step("charge")
        step._event["startedAt"] = "2020-01-01T00:00:00Z"
        step._event["finishedAt"] = "2020-01-01T00:00:03Z"
        report = asyncio.create_task(step.success("charged", wait_for=True))
        event = await ws.sent.get()
        assert event["invocationId"] == grant.invocation_id == event["idempotencyKey"]
        assert event["finishedAt"] == "2020-01-01T00:00:03Z"
        assert event["waitFor"] is True
        await ws.reply(
            event, stepId="event-1", validation={"decision": "passed", "stepId": "event-1"}
        )
        result = await report
        assert result.step_id == "event-1" and result.validation.decision == "passed"
        assert result.timestamp == event["finishedAt"]
    finally:
        await cleanup(ws, conn)


@pytest.mark.asyncio
async def test_concurrent_waits_can_finish_out_of_order():
    ws, conn, thread = connected()
    try:
        first = asyncio.create_task(thread.wait_for("first"))
        second = asyncio.create_task(thread.wait_for("second"))
        req1, req2 = await ws.sent.get(), await ws.sent.get()
        await ws.reply(req2, decision="allowed", invocationId=req2["invocationId"])
        assert (await second).step_name == "second"
        assert not first.done()
        await ws.reply(req1, decision="allowed", invocationId=req1["invocationId"])
        assert (await first).step_name == "first"
    finally:
        await cleanup(ws, conn)


@pytest.mark.asyncio
async def test_timeout_cancellation_and_disconnect_clear_waiters():
    ws, conn, thread = connected()
    try:
        pending = asyncio.create_task(thread.wait_for("blocked", WaitOptions(timeout=0.02)))
        req = await ws.sent.get()
        with pytest.raises(ThreadifyError) as error:
            await pending
        assert error.value.code == "THREADIFY_WAIT_TIMEOUT"
        assert error.value.invocation_id == req["invocationId"]
        assert (await ws.sent.get())["targetRequestId"] == req["requestId"]
        assert not conn._requests
        pending = asyncio.create_task(thread.wait_for("blocked"))
        req = await ws.sent.get()
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert (await ws.sent.get())["targetRequestId"] == req["requestId"]
        assert not conn._requests
        pending = asyncio.create_task(thread.wait_for("blocked"))
        await ws.sent.get()
        legacy = asyncio.create_task(conn._wait_response(lambda _: False))
        await ws.close()
        for task in (pending, legacy):
            with pytest.raises(ConnectionError):
                await asyncio.wait_for(task, 0.2)
    finally:
        await cleanup(ws, conn)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response,code",
    [
        ({"decision": "pending"}, "THREADIFY_SYNC_WAIT_UNSUPPORTED"),
        ({"decision": "passed", "stepId": "wrong"}, "THREADIFY_INVALID_WAIT_RESPONSE"),
        ({"decision": "violated", "stepId": "event-1"}, "THREADIFY_VALIDATION_VIOLATED"),
        ({"decision": "unvalidated", "stepId": "event-1"}, "THREADIFY_VALIDATION_UNAVAILABLE"),
    ],
)
async def test_validation_rejects_nonfinal_or_wrong_result(response, code):
    ws, conn, thread = connected()
    try:
        pending = asyncio.create_task(thread.wait_for_validation("charge", "event-1"))
        req = await ws.sent.get()
        await ws.reply(req, **response)
        with pytest.raises(ThreadifyError) as error:
            await pending
        assert error.value.code == code
        assert error.value.step_id == "event-1"
    finally:
        await cleanup(ws, conn)


@pytest.mark.asyncio
async def test_wrong_invocation_and_duplicate_report_do_not_grant_success():
    ws, conn, thread = connected()
    try:
        pending = asyncio.create_task(thread.wait_for("charge"))
        req = await ws.sent.get()
        await ws.reply(req, decision="allowed", invocationId="other")
        with pytest.raises(ThreadifyError, match="different invocation"):
            await pending
        step = thread.step("charge")
        pending = asyncio.create_task(step.success(wait_for=True))
        req = await ws.sent.get()
        await ws.reply(req, status="error", isDuplicate=True, message="duplicate")
        with pytest.raises(ThreadifyError) as error:
            await pending
        assert error.value.idempotency_key == req["idempotencyKey"]
    finally:
        await cleanup(ws, conn)


@pytest.mark.asyncio
async def test_grant_cancel_releases_permission():
    ws, conn, thread = connected()
    try:
        pending = asyncio.create_task(thread.wait_for("charge"))
        req = await ws.sent.get()
        await ws.reply(req, decision="allowed", invocationId=req["invocationId"])
        grant = await pending
        pending = asyncio.create_task(grant.cancel())
        req = await ws.sent.get()
        assert req["cancel"] is True
        await ws.reply(req, decision="cancelled", invocationId=grant.invocation_id)
        assert (await pending).decision == "cancelled"
        assert "charge" not in thread._invocation_grants
    finally:
        await cleanup(ws, conn)


@pytest.mark.asyncio
async def test_otel_exporter_uses_recorded_span_and_event_times():
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    from threadify.otel_exporter import ThreadifySpanExporter

    provider = TracerProvider()
    spans = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(spans))
    tracer = provider.get_tracer("parity")
    with tracer.start_as_current_span("parent"):
        span = tracer.start_span("db.select", start_time=1577836800000000000)
        span.set_attribute("threadify.ref.order_id", "ORD-1001")
        span.add_event("row", timestamp=1577836801000000000)
        span.end(end_time=1577836803000000000)
    recorded = spans.get_finished_spans()[0]
    ws, conn, thread = connected()
    try:
        exporter = ThreadifySpanExporter(conn)
        exporter._get_or_resolve_thread = AsyncMock(return_value=thread)
        task = asyncio.create_task(exporter._do_process_span(recorded))
        req = await ws.sent.get()
        # OTEL's ordinary addRefs and event acknowledgements retain legacy routing.
        await ws.incoming.put({"action": req["action"], "status": "success"})
        event = await ws.sent.get()
        assert event["startedAt"].startswith("2020-01-01T00:00:00")
        assert event["finishedAt"].startswith("2020-01-01T00:00:03")
        assert event["subSteps"][0]["recordedAt"].startswith("2020-01-01T00:00:01")
        await ws.incoming.put({"action": event["action"], "status": "success"})
        await task
    finally:
        await cleanup(ws, conn)
        provider.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,code",
    [
        (429, "THREADIFY_ALLOWANCE_EXCEEDED"),
        (503, "THREADIFY_LICENSE_UNAVAILABLE"),
        (401, "THREADIFY_HTTP_ERROR"),
    ],
)
async def test_http_policy_errors_keep_status_and_code(monkeypatch, status, code):
    from types import SimpleNamespace

    import httpx

    from threadify import Threadify
    from threadify.data_retriever import GraphQLClient

    rejection = RuntimeError("upgrade denied")
    rejection.response = SimpleNamespace(status_code=status)
    monkeypatch.setattr("threadify.client.websockets.connect", AsyncMock(side_effect=rejection))
    with pytest.raises(ThreadifyError) as error:
        await Threadify.connect("fixture-key", engine_url="https://example.com")
    assert (error.value.status, error.value.code) == (status, code)
    client = GraphQLClient("https://example.com/graphql", "fixture-key")
    await client._client.aclose()
    client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(status, text="policy denied"))
    )
    try:
        with pytest.raises(ThreadifyError) as error:
            await client.query("query { threads { id } }")
        assert (error.value.status, error.value.code) == (status, code)
    finally:
        await client.close()
