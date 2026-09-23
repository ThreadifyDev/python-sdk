# OTel thread keys

The exporter selects an explicit `threadify.thread_id`, then
`threadify.thread_key`, then `workflow.run_id`, then trace-only correlation.
Different traces with the same thread key share a thread and its pinned contract.
Conflicting keys or contracts and closed threads are rejected by the Engine.

```python
await connection.thread("agent-session:123", {
    "label": "Agent session",
    "contract": "agent_contract",
    "refs": {"customer_id": "123"},
})
exporter = connection.create_span_exporter({"useWorkflowRunId": False})

# Set this attribute on each turn, MCP call, and tool-call span.
span.set_attribute("threadify.thread_key", "agent-session:123")
```

The exporter calls `connection.thread(thread_key, options)` for keyed spans.
Initialize contracted sessions before reporting spans. Later spans omit
`threadify.contract` and load the stored contract version; explicitly supplying a
different contract fails without creating a replacement thread.

Omitting `useWorkflowRunId` enables workflow correlation. Setting it to `False`
ignores only `workflow.run_id`; explicit thread keys still apply. Set thread-key
attributes on all relevant spans, or a resource dedicated to that run. Keys are
trimmed, case-sensitive strings of at most 1024 UTF-8 bytes. Explicit thread keys
must not be blank; blank workflow run IDs fall through to trace-only correlation.
Use a unique key per logical run rather than a category.

Creation attributes include `threadify.label`, `threadify.contract`,
`threadify.service`, `threadify.role`, and `threadify.tags`. Step references use
`threadify.ref.*`, and `threadify.context.*` attributes become step context.
Root spans of keyed sessions do not finish the whole thread. For a free-form
keyed session, the Boolean span attribute `threadify.run.complete = True`
explicitly completes the run after all spans in that batch are recorded.
Contracted threads finish through their contract; the exporter does not close
them when a root span or explicit run-complete marker arrives. Explicit internal
thread IDs are also never automatically completed by the exporter.

Trace/span IDs remain in step context and idempotency keys. A late conflicting
identity is rejected; existing stored threads are not merged. Trace-only spans
retain the internal trace correlation path, without inventing an application
thread key, and a free-form root can finish that trace's thread.

This WebSocket exporter requires the matching updated Engine. Standard
OTLP/HTTP exporters may use `/v1/traces?use_workflow_run_id=false`; both transports
share the Engine resolver.

Run `python -m unittest discover -s tests -p test_otel_correlation.py` with the
OTel optional dependencies installed.

Use `BatchSpanProcessor` so synchronous export runs off the connection's event
loop. `export()` returns failure for rejected or timed-out batches and attempts
remaining unrelated spans. A failed batch never triggers its completion markers.
Flush or shut down the provider with `await asyncio.to_thread(provider.force_flush)`
or `await asyncio.to_thread(provider.shutdown)` while the connection loop is still
running; then close the connection. Exporting synchronously on that event loop
returns failure instead of blocking it.
