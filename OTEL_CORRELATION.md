# OTel thread references

The exporter selects an explicit `threadify.thread_id`, then `threadify.external_ref`, then `workflow.run_id`, then the trace ID. Different traces with the same external/workflow reference share a thread and its contract. Conflicting references or contracts are rejected by the Engine.

```python
exporter = ThreadifySpanExporter(connection, options={"useWorkflowRunId": False})
```

Omitting the option enables workflow correlation. Setting it to `False` ignores only `workflow.run_id`; explicit external references still apply. Set reference attributes on all relevant spans, or a resource dedicated to that run. References are trimmed, case-sensitive strings of at most 1024 UTF-8 bytes. Blank values fall through to the next choice. Use a unique reference per logical run rather than a category.

Shared roots do not finish the whole thread; use the Boolean attribute `threadify.run.complete = True` to explicitly finish the run. Trace/span IDs remain in step context and idempotency keys. A late conflicting identity is rejected; existing stored threads are not merged.

This WebSocket exporter requires the matching updated Engine. Standard OTLP/HTTP exporters may use `/v1/traces?use_workflow_run_id=false`; both transports share the Engine resolver.

Run `python -m unittest discover -s tests -p test_otel_correlation.py` with the OTel optional dependencies installed.
