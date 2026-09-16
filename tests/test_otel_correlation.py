import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from threadify.otel_exporter import ThreadifySpanExporter


def span(attrs, trace=1, resource=None):
    return SimpleNamespace(name='tool.call', attributes=attrs, resource=SimpleNamespace(attributes=resource or {}), get_span_context=lambda: SimpleNamespace(trace_id=trace, span_id=2), events=[], status=None, start_time=None, end_time=None, parent=None)


class CorrelationTests(unittest.IsolatedAsyncioTestCase):
    def fixture(self, options=None):
        thread = Mock()
        thread.add_refs = AsyncMock()
        thread.complete = AsyncMock()
        thread.close = AsyncMock()
        step = thread.step.return_value
        step.success = AsyncMock()
        step.failed = AsyncMock()
        conn = SimpleNamespace(service_name='worker', start=AsyncMock(return_value=thread))
        return ThreadifySpanExporter(conn, options), conn, thread

    def test_selection(self):
        exporter, _, _ = self.fixture()
        self.assertEqual(exporter._external_ref(span({'workflow.run_id': 'run'})), 'run')
        self.assertEqual(exporter._external_ref(span({}, resource={'workflow.run_id': 'resource'})), 'resource')
        self.assertEqual(exporter._external_ref(span({'threadify.external_ref': ' explicit ', 'workflow.run_id': 'run'})), 'explicit')
        self.assertIsNone(exporter._external_ref(span({'threadify.thread_id': 'internal', 'workflow.run_id': 4})))
        disabled, _, _ = self.fixture({'useWorkflowRunId': False})
        self.assertIsNone(disabled._external_ref(span({'workflow.run_id': 'run'})))
        self.assertEqual(disabled._external_ref(span({'threadify.external_ref': 'explicit'})), 'explicit')
        with self.assertRaises(TypeError):
            exporter._external_ref(span({'workflow.run_id': 4}))
        with self.assertRaises(ValueError):
            exporter._external_ref(span({'workflow.run_id': 'é' * 513}))

    async def test_shared_spans_keep_open_and_revalidate(self):
        exporter, conn, thread = self.fixture()
        await exporter._do_process_span(span({'workflow.run_id': 'run', 'threadify.contract': 'a', 'threadify.ref.threadify.external_ref': 'spoof'}))
        await exporter._do_process_span(span({'workflow.run_id': 'run', 'threadify.contract': 'b'}, trace=3))
        self.assertEqual(conn.start.await_count, 2)
        self.assertEqual(conn.start.await_args.kwargs['refs']['threadify.external_ref'], 'run')
        self.assertEqual(conn.start.await_args.kwargs['contract_name'], 'b')
        thread.complete.assert_not_awaited()
        self.assertNotIn('otel_trace_id', thread.add_refs.await_args.args[0])
        self.assertNotIn('threadify.external_ref', thread.add_refs.await_args.args[0])
        keys = thread.step.return_value.idempotency_key.call_args_list
        self.assertNotEqual(keys[0].args, keys[1].args)
        await exporter._do_process_span(span({'workflow.run_id': 'run', 'threadify.run.complete': True}, trace=4))
        thread.complete.assert_awaited_once()
        conn.start.side_effect = RuntimeError('contract conflicts')
        with self.assertRaisesRegex(RuntimeError, 'contract conflicts'):
            await exporter._do_process_span(span({'workflow.run_id': 'run'}, trace=5))
