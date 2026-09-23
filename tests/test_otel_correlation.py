import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from threadify.otel_exporter import ThreadifySpanExporter


def span(attrs, trace=1, resource=None):
    return SimpleNamespace(
        name="tool.call",
        attributes=attrs,
        resource=SimpleNamespace(attributes=resource or {}),
        get_span_context=lambda: SimpleNamespace(trace_id=trace, span_id=2),
        events=[],
        status=None,
        start_time=None,
        end_time=None,
        parent=None,
    )


class CorrelationTests(unittest.IsolatedAsyncioTestCase):
    def fixture(self, options=None):
        thread = Mock(thread_id="thread-id", thread_key=None, contract_id="", contract_name="")
        thread.add_refs = AsyncMock()
        thread.complete = AsyncMock()
        thread.close = AsyncMock()
        step = thread.step.return_value
        step._event = {}
        step.success = AsyncMock()
        step.failed = AsyncMock()
        conn = SimpleNamespace(
            service_name="worker",
            thread=AsyncMock(return_value=thread),
            start=AsyncMock(return_value=thread),
            join=AsyncMock(return_value=thread),
        )
        return ThreadifySpanExporter(conn, options), conn, thread

    def test_selection(self):
        exporter, _, _ = self.fixture()
        self.assertEqual(exporter._thread_key(span({"workflow.run_id": "run"})), "run")
        self.assertEqual(
            exporter._thread_key(span({}, resource={"workflow.run_id": "resource"})), "resource"
        )
        self.assertEqual(
            exporter._thread_key(
                span({"threadify.thread_key": " explicit ", "workflow.run_id": "run"})
            ),
            "explicit",
        )
        self.assertIsNone(
            exporter._thread_key(span({"threadify.thread_id": "internal", "workflow.run_id": 4}))
        )
        disabled, _, _ = self.fixture({"useWorkflowRunId": False})
        self.assertIsNone(disabled._thread_key(span({"workflow.run_id": "run"})))
        self.assertEqual(
            disabled._thread_key(span({"threadify.thread_key": "explicit"})), "explicit"
        )
        with self.assertRaises(ValueError):
            exporter._thread_key(span({"threadify.thread_key": " "}))
        with self.assertRaises(TypeError):
            exporter._thread_key(span({"workflow.run_id": 4}))
        with self.assertRaises(ValueError):
            exporter._thread_key(span({"workflow.run_id": "é" * 513}))

    async def test_shared_spans_keep_open_and_revalidate(self):
        exporter, conn, thread = self.fixture()
        await exporter._do_process_span(
            span(
                {
                    "workflow.run_id": "run",
                    "threadify.contract": "a",
                    "threadify.ref.threadify.thread_key": "spoof",
                }
            )
        )
        await exporter._do_process_span(
            span({"workflow.run_id": "run", "threadify.contract": "b"}, trace=3)
        )
        self.assertEqual(conn.thread.await_count, 2)
        self.assertEqual(conn.thread.await_args.args[0], "run")
        self.assertEqual(conn.thread.await_args.args[1]["contract"], "b")
        conn.start.assert_not_awaited()
        thread.complete.assert_not_awaited()
        self.assertNotIn("otel_trace_id", thread.add_refs.await_args.args[0])
        self.assertNotIn("threadify.thread_key", thread.add_refs.await_args.args[0])
        keys = thread.step.return_value.idempotency_key.call_args_list
        self.assertNotEqual(keys[0].args, keys[1].args)
        await exporter._do_process_span(
            span({"workflow.run_id": "run", "threadify.run.complete": True}, trace=4)
        )
        thread.complete.assert_awaited_once()
        self.assertNotIn("contract", conn.thread.await_args.args[1])
        conn.thread.side_effect = RuntimeError("contract conflicts")
        with self.assertRaisesRegex(RuntimeError, "contract conflicts"):
            await exporter._do_process_span(span({"workflow.run_id": "run"}, trace=5))
        conn.start.assert_not_awaited()

    async def test_resumed_stored_contract_prevents_exporter_completion(self):
        exporter, conn, thread = self.fixture()
        thread.contract_id = "stored-contract"
        thread.contract_name = "agent"
        await exporter._do_process_span(
            span({"threadify.thread_key": "session", "threadify.run.complete": True})
        )
        self.assertNotIn("contract", conn.thread.await_args.args[1])
        thread.complete.assert_not_awaited()
        thread.close.assert_not_awaited()

    async def test_omitted_key_in_same_trace_resumes_previous_key(self):
        exporter, conn, thread = self.fixture()
        await exporter._do_process_span(span({"threadify.thread_key": "session"}))
        await exporter._do_process_span(span({}))
        self.assertEqual(conn.thread.await_count, 2)
        self.assertEqual(conn.thread.await_args.args[0], "session")
        conn.start.assert_not_awaited()
        thread.complete.assert_not_awaited()

    async def test_closed_key_error_does_not_record_or_fall_back(self):
        exporter, conn, thread = self.fixture()
        conn.thread.side_effect = RuntimeError("thread is closed")
        with self.assertRaisesRegex(RuntimeError, "thread is closed"):
            await exporter._do_process_span(span({"threadify.thread_key": "session"}))
        conn.start.assert_not_awaited()
        thread.step.assert_not_called()

    async def test_batch_completes_only_after_all_steps(self):
        exporter, _, thread = self.fixture()
        events = []
        thread.step.return_value.success.side_effect = lambda _: events.append("step")
        thread.complete.side_effect = lambda _: events.append("complete")
        await exporter._process_all(
            [
                span({"threadify.thread_key": "session", "threadify.run.complete": True}),
                span({"threadify.thread_key": "session"}, trace=2),
            ]
        )
        self.assertEqual(events, ["step", "step", "complete"])

    async def test_internal_id_is_joined_and_never_completed_by_root(self):
        exporter, conn, thread = self.fixture()
        await exporter._do_process_span(span({"threadify.thread_id": "internal"}))
        conn.join.assert_awaited_once_with("internal", "participant")
        conn.thread.assert_not_awaited()
        conn.start.assert_not_awaited()
        thread.complete.assert_not_awaited()

    async def test_trace_only_compatibility_still_uses_trace_correlation(self):
        exporter, conn, thread = self.fixture()
        await exporter._do_process_span(span({}))
        self.assertEqual(conn.start.await_args.kwargs["refs"], {"otel_trace_id": "0" * 31 + "1"})
        conn.thread.assert_not_awaited()
        thread.complete.assert_awaited_once()

    async def test_restarted_exporter_recovers_key_from_trace_binding(self):
        exporter, conn, thread = self.fixture()
        thread.thread_key = "session"
        await exporter._do_process_span(span({}))
        conn.start.assert_awaited_once()
        thread.complete.assert_not_awaited()
        self.assertNotIn("otel_trace_id", thread.add_refs.await_args.args[0])
        await exporter._do_process_span(span({}))
        self.assertEqual(conn.thread.await_args.args[0], "session")


class ExportResultTests(unittest.IsolatedAsyncioTestCase):
    fixture = CorrelationTests.fixture

    async def test_worker_export_reports_failure_and_continues_unrelated_spans(self):
        import asyncio

        from opentelemetry.sdk.trace.export import SpanExportResult

        exporter, conn, thread = self.fixture()
        conn.is_connected = True
        conn.thread.side_effect = [RuntimeError("closed session"), thread]
        with self.assertLogs("threadify.otel", level="ERROR"):
            result = await asyncio.to_thread(
                exporter.export,
                [
                    span({"threadify.thread_key": "closed"}),
                    span({"threadify.thread_key": "open"}, trace=3),
                ],
            )
        self.assertEqual(result, SpanExportResult.FAILURE)
        self.assertEqual(conn.thread.await_count, 2)
        thread.step.return_value.success.assert_awaited_once()
        thread.complete.assert_not_awaited()
        self.assertFalse(await asyncio.to_thread(exporter.force_flush))

    async def test_export_waits_for_ack_and_flush_tracks_pending_work(self):
        import asyncio

        from opentelemetry.sdk.trace.export import SpanExportResult

        exporter, conn, thread = self.fixture()
        conn.is_connected = True
        entered, release = asyncio.Event(), asyncio.Event()

        async def delayed(*args):
            entered.set()
            await release.wait()
            return thread

        conn.thread.side_effect = delayed
        task = asyncio.create_task(
            asyncio.to_thread(exporter.export, [span({"threadify.thread_key": "open"})])
        )
        await entered.wait()
        self.assertFalse(task.done())
        self.assertFalse(await asyncio.to_thread(exporter.force_flush, 1))
        release.set()
        self.assertEqual(await task, SpanExportResult.SUCCESS)
        self.assertTrue(await asyncio.to_thread(exporter.force_flush))

    async def test_same_loop_export_fails_without_deadlocking_or_claiming_success(self):
        from opentelemetry.sdk.trace.export import SpanExportResult

        exporter, conn, _ = self.fixture()
        conn.is_connected = True
        with self.assertLogs("threadify.otel", level="ERROR"):
            result = exporter.export([span({"threadify.thread_key": "open"})])
        self.assertEqual(result, SpanExportResult.FAILURE)
        conn.thread.assert_not_awaited()

    async def test_worker_timeout_and_shutdown_are_observable(self):
        import asyncio

        from opentelemetry.sdk.trace.export import SpanExportResult

        exporter, conn, _ = self.fixture()
        conn.is_connected = True
        cancelled = asyncio.Event()

        async def blocked(*args):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        conn.thread.side_effect = blocked
        with self.assertLogs("threadify.otel", level="ERROR"):
            self.assertEqual(
                await asyncio.to_thread(
                    exporter.export, [span({"threadify.thread_key": "open"})], 10
                ),
                SpanExportResult.FAILURE,
            )
            await cancelled.wait()
            await asyncio.to_thread(exporter.shutdown)
        self.assertEqual(await asyncio.to_thread(exporter.export, []), SpanExportResult.FAILURE)

    async def test_batch_processor_flushes_before_connection_loop_shutdown(self):
        import asyncio

        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        exporter, conn, thread = self.fixture()
        conn.is_connected = True
        provider = TracerProvider()
        provider.add_span_processor(BatchSpanProcessor(exporter))
        try:
            with provider.get_tracer("regression").start_as_current_span(
                "tool", attributes={"threadify.thread_key": "session"}
            ):
                pass
            self.assertTrue(await asyncio.to_thread(provider.force_flush))
            thread.step.return_value.success.assert_awaited_once()
            self.assertTrue(await asyncio.to_thread(exporter.force_flush))
        finally:
            await asyncio.to_thread(provider.shutdown)
