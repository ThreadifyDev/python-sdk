from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from opentelemetry.sdk.trace import ReadableSpan

    from threadify.connection import Connection

logger = logging.getLogger("threadify.otel")

# Span status codes (OpenTelemetry proto)
_STATUS_UNSET = 0
_STATUS_OK = 1
_STATUS_ERROR = 2

# Attempt to inherit from SpanExporter when OTel is installed.
# This makes isinstance() checks in BatchSpanProcessor work correctly.
try:
    from opentelemetry.sdk.trace.export import SpanExporter as _SpanExporterBase
except ImportError:
    _SpanExporterBase = object  # type: ignore[misc, assignment]


def _require_otel() -> None:
    """Raise ImportError with a helpful message if OpenTelemetry is not installed."""
    try:
        import opentelemetry.sdk.trace.export  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "OpenTelemetry is required for ThreadifySpanExporter. "
            "Install it: pip install opentelemetry-api opentelemetry-sdk"
        ) from exc


class ThreadifySpanExporter(_SpanExporterBase):
    """OpenTelemetry SpanExporter that auto-translates Spans into Threadify Threads/Steps.

    This hooks into the OpenTelemetry SDK and creates Threadify threads and steps
    from span data, enabling zero-instrumentation observability for existing
    OpenTelemetry-instrumented applications.

    Usage::

        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from threadify import Threadify

        conn = await Threadify.connect("api-key", service_name="my-service", ...)
        thread = await conn.thread("order:ORD-123", {"label": "Order 123"})

        exporter = thread.create_span_exporter(options={"refs": ["orderId"]})

        provider = TracerProvider()
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
    """

    def __init__(self, connection: Connection, options: dict[str, Any] | None = None):
        _require_otel()
        self._connection = connection
        self._options = options or {}
        if not isinstance(self._options.get("useWorkflowRunId", True), bool):
            raise TypeError("useWorkflowRunId must be a boolean")

        # Normalise refs to a mapping {attribute_key: ref_key}
        refs = self._options.get("refs", [])
        if isinstance(refs, list):
            self._refs_map: dict[str, str] = {k: k for k in refs}
        elif isinstance(refs, dict):
            self._refs_map = dict(refs)
        else:
            self._refs_map = {}

        # Span-name filters; e.g. ["invoke_llm", "adk.before*", "llm.*"]
        self._filters: list[str] = self._options.get("filters", [])

        # trace_id -> asyncio.Future[ThreadInstance]
        self._trace_threads: dict[str, asyncio.Future[Any]] = {}
        self._resolve_lock = asyncio.Lock()
        self._trace_references: dict[str, str] = {}

        self._pending: set[concurrent.futures.Future] = set()
        self._pending_lock = threading.Lock()
        self._flush_failed = False
        self._shutdown = False

        # Capture the event loop so we can schedule coroutines from sync export().
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None

    def export(self, spans: list[ReadableSpan], timeout_millis: float = 30000) -> Any:
        """Export a batch of spans.

        Called by the OpenTelemetry ``BatchSpanProcessor`` (typically from a
        worker thread). We schedule the async work on the captured event loop.
        """
        if not self._connection.is_connected:
            return self._make_result(1, "Threadify connection is not open")

        if self._loop is None or not self._loop.is_running():
            return self._make_result(1, "No running event loop available")

        if self._on_connection_loop():
            logger.error(
                "Synchronous export cannot run on the connection event loop; use BatchSpanProcessor"
            )
            return self._make_result(
                1, "Use BatchSpanProcessor to export off the connection event loop"
            )

        with self._pending_lock:
            if self._shutdown:
                return self._make_result(1, "Exporter is shut down")
            future = asyncio.run_coroutine_threadsafe(self._process_all(spans), self._loop)
            self._pending.add(future)
        future.add_done_callback(self._export_done)
        try:
            future.result(timeout=max(0, timeout_millis) / 1000)
        except concurrent.futures.TimeoutError:
            future.cancel()
            logger.error("Timed out exporting Threadify spans")
            return self._make_result(1, "Export timed out")
        except Exception:
            return self._make_result(1, "Export failed")
        return self._make_result(0)

    def _on_connection_loop(self) -> bool:
        try:
            return asyncio.get_running_loop() is self._loop
        except RuntimeError:
            return False

    def _export_done(self, future: concurrent.futures.Future) -> None:
        failed = future.cancelled() or future.exception() is not None
        with self._pending_lock:
            self._flush_failed |= failed
            self._pending.discard(future)
        if failed:
            logger.error("Threadify span batch export failed")

    def force_flush(self, timeout_millis: float = 30000) -> bool:
        """Wait for exports accepted before this call, without blocking their event loop."""
        deadline = time.monotonic() + max(0, timeout_millis) / 1000
        with self._pending_lock:
            pending = tuple(self._pending)
        if pending and self._on_connection_loop():
            logger.error("Flush must run off the connection event loop while exports are pending")
            return False
        successful = True
        for future in pending:
            try:
                future.result(timeout=max(0, deadline - time.monotonic()))
            except Exception:
                successful = False
        with self._pending_lock:
            successful = successful and not self._flush_failed
            self._flush_failed = False
        return successful

    def shutdown(self) -> None:
        """Stop accepting batches and drain pending exports; connection ownership stays external."""
        with self._pending_lock:
            self._shutdown = True
        if not self.force_flush():
            logger.error("Exporter shutdown did not flush all spans successfully")

    # --- internals ---

    async def _process_all(self, spans: list[ReadableSpan]) -> None:
        completions = {}
        failures = []
        for span in spans:
            if self._should_drop(span.name):
                continue
            try:
                await self._do_process_span(span, completions)
            except Exception as error:
                logger.exception("Failed to process span")
                failures.append(error)
        # A failed batch must not close sessions that may need a retry.
        if failures:
            raise RuntimeError(f"Failed to export {len(failures)} span(s)") from failures[0]
        for complete in completions.values():
            await complete()

    async def _process_span(self, span: ReadableSpan) -> None:
        try:
            await self._do_process_span(span)
        except Exception:
            logger.exception("Failed to process span")

    async def _do_process_span(self, span: ReadableSpan, completions: dict | None = None) -> None:
        ctx = span.get_span_context()
        trace_id = format(ctx.trace_id, "032x")
        span_id = format(ctx.span_id, "016x")
        thread = await self._get_or_resolve_thread(span, trace_id)

        # Step name
        step_name = self._span_attr(span, "threadify.step_name") or span.name
        step = thread.step(step_name)
        step.idempotency_key(f"otel:{trace_id}:{span_id}")

        # Separate attributes into context / refs
        context: dict[str, str] = {}
        refs: dict[str, str] = {
            "otel_trace_id": trace_id,
            "otel_span_id": span_id,
        }

        if self._thread_key(span):
            refs.pop("otel_trace_id", None)
        context["otel.trace_id"] = trace_id
        context["otel.span_id"] = span_id
        for key, value in span.attributes.items():
            # Skip internal threadify directives
            if key in {
                "threadify.thread_key",
                "threadify.thread_id",
                "threadify.contract",
                "threadify.label",
                "threadify.step_name",
                "threadify.role",
                "threadify.service",
                "threadify.tags",
            }:
                continue

            str_value = str(value)
            if key in self._refs_map or key.startswith("threadify.ref."):
                ref_key = (
                    key.replace("threadify.ref.", "")
                    if key.startswith("threadify.ref.")
                    else self._refs_map[key]
                )
                if ref_key != "threadify.thread_key":
                    refs[ref_key] = str_value
            elif key.startswith("threadify.context."):
                context[key.replace("threadify.context.", "")] = str_value
            else:
                context[key] = str_value

        if context:
            step.add_context(context)
        if refs:
            await thread.add_refs(refs)

        # Map timing (OTel uses nanoseconds since epoch)
        start_time_ns = span.start_time
        end_time_ns = span.end_time
        if start_time_ns:
            step._event["startedAt"] = _ns_to_iso(start_time_ns)
        if end_time_ns:
            step._event["finishedAt"] = _ns_to_iso(end_time_ns)

        # Map span events to sub-steps
        for event in span.events:
            event_time_ns = event.timestamp
            recorded_at = _ns_to_iso(event_time_ns) if event_time_ns else _now_iso()
            payload: dict[str, Any] = {}
            if event.attributes:
                payload = dict(event.attributes)
            step.sub_step(
                name=event.name,
                data=payload,
                status="success",
            )
            # Update recordedAt on the last sub-step data
            if step._sub_steps:
                step._sub_steps[-1].recorded_at = recorded_at

        # Map status
        target_status = STATUS_SUCCESS
        message = ""
        if span.status:
            message = span.status.description or ""
            try:
                from opentelemetry.trace.status import StatusCode

                if span.status.status_code is StatusCode.ERROR:
                    target_status = STATUS_FAILED
            except Exception:
                # Defensive: fallback to raw int if enum isn't available
                if getattr(span.status.status_code, "value", 0) == _STATUS_ERROR:
                    target_status = STATUS_FAILED

        if target_status == STATUS_SUCCESS:
            await step.success(message or "")
        else:
            await step.failed(message or "Span ended with error status")

        # Root span auto-complete
        parent_ctx = getattr(span, "parent", None)
        parent_span_id = format(parent_ctx.span_id, "016x") if parent_ctx else None
        correlated = bool(self._thread_key(span))
        attrs = {
            **(getattr(getattr(span, "resource", None), "attributes", {}) or {}),
            **span.attributes,
        }
        if (
            not attrs.get("threadify.thread_id")
            and not thread.contract_name
            and not thread.contract_id
            and (
                (not parent_span_id and not correlated)
                or (correlated and span.attributes.get("threadify.run.complete") is True)
            )
        ):

            async def complete():
                if target_status == STATUS_SUCCESS:
                    await thread.complete("Root span completed successfully")
                else:
                    await thread.close("Root span failed")
                self._trace_threads.pop(trace_id, None)

            if completions is not None:
                completions[thread.thread_id] = complete
            else:
                await complete()

    def _thread_key(self, span: ReadableSpan) -> str | None:
        """Resolve the application thread key, preserving explicit internal targets."""
        resource = getattr(getattr(span, "resource", None), "attributes", {}) or {}
        attrs = {**resource, **span.attributes}
        if attrs.get("threadify.thread_id"):
            return None
        keys = ["threadify.thread_key"]
        if self._options.get("useWorkflowRunId", True):
            keys.append("workflow.run_id")
        for key in keys:
            if key not in attrs:
                continue
            value = attrs[key]
            if not isinstance(value, str):
                raise TypeError(f"{key} must be a string")
            value = value.strip()
            if len(value.encode("utf-8")) > 1024:
                raise ValueError(f"{key} exceeds 1024 bytes")
            if value:
                return value
            if key == "threadify.thread_key":
                raise ValueError("threadKey must be a non-empty string")
        trace_id = format(span.get_span_context().trace_id, "032x")
        return self._trace_references.get(trace_id)

    async def _get_or_resolve_thread(self, span: ReadableSpan, trace_id: str) -> Any:
        """Get or create a ThreadInstance for this trace."""
        async with self._resolve_lock:
            return await self._resolve_thread(span, trace_id)

    async def _resolve_thread(self, span: ReadableSpan, trace_id: str) -> Any:
        thread_key = self._thread_key(span)
        if thread_key:
            attrs = {
                **(getattr(getattr(span, "resource", None), "attributes", {}) or {}),
                **span.attributes,
            }
            options = {
                "label": attrs.get("threadify.label") or span.name,
                "refs": {"otel_trace_id": trace_id},
            }
            for attribute, option in (
                ("threadify.contract", "contract"),
                ("threadify.service", "service_name"),
                ("threadify.role", "role"),
            ):
                if attribute in attrs:
                    options[option] = attrs[attribute]
            if "threadify.tags" in attrs:
                tags = attrs["threadify.tags"]
                options["tags"] = (
                    [tag.strip() for tag in tags.split(",") if tag.strip()]
                    if isinstance(tags, str)
                    else list(tags)
                )
            thread = await self._connection.thread(thread_key, options)
            if trace_id not in self._trace_references:
                self._trace_references[trace_id] = thread_key
                asyncio.get_running_loop().call_later(
                    600, self._trace_references.pop, trace_id, None
                )
            return thread
        if trace_id not in self._trace_threads:
            fut: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
            self._trace_threads[trace_id] = fut

            try:
                existing_thread_id = (
                    {
                        **(getattr(getattr(span, "resource", None), "attributes", {}) or {}),
                        **span.attributes,
                    }
                ).get("threadify.thread_id")
                if existing_thread_id:
                    role = self._span_attr(span, "threadify.role") or "participant"
                    thread = await self._connection.join(existing_thread_id, role)
                else:
                    contract_name = self._span_attr(span, "threadify.contract")
                    label = self._span_attr(span, "threadify.label") or span.name
                    service_name = (
                        self._span_attr(span, "threadify.service") or self._connection.service_name
                    )
                    role = self._span_attr(span, "threadify.role") or "participant"

                    tags = self._span_attr_list(span, "threadify.tags")
                    # Trace-only ingestion has no application key. Keep its existing
                    # trace correlation without creating a synthetic threadKey.
                    thread = await self._connection.start(
                        label=label,
                        contract_name=contract_name or "",
                        service_name=service_name,
                        tags=tags,
                        role=role,
                        refs={"otel_trace_id": trace_id},
                    )
                fut.set_result(thread)
            except Exception as exc:
                self._trace_threads.pop(trace_id, None)
                fut.set_exception(exc)
                return await fut

            # Memory-leak safety: remove after 10 minutes
            asyncio.get_event_loop().call_later(600, self._trace_threads.pop, trace_id, None)

        thread = await self._trace_threads[trace_id]
        # A restarted exporter may first receive a span with only its trace ID.
        # Recover the persisted key so this root cannot finish a shared session.
        if thread.thread_key and trace_id not in self._trace_references:
            self._trace_references[trace_id] = thread.thread_key
            asyncio.get_running_loop().call_later(600, self._trace_references.pop, trace_id, None)
        return thread

    def _should_drop(self, name: str) -> bool:
        for f in self._filters:
            if not f:
                continue
            if f.endswith("*"):
                if name.startswith(f[:-1]):
                    return True
                continue
            if name == f:
                return True
        return False

    @staticmethod
    def _span_attr(span: ReadableSpan, key: str) -> str | None:
        value = span.attributes.get(key)
        return str(value) if value is not None else None

    @staticmethod
    def _span_attr_list(span: ReadableSpan, key: str) -> list[str] | None:
        value = span.attributes.get(key)
        if value is None:
            return None
        if isinstance(value, list):
            return [str(v) for v in value]
        if isinstance(value, str):
            # Allow comma-separated tags as a fallback
            return [v.strip() for v in value.split(",") if v.strip()]
        return None

    @staticmethod
    def _make_result(code: int, error: str | None = None) -> Any:
        """Build an OpenTelemetry ExportResult-compatible object."""
        try:
            from opentelemetry.sdk.trace.export import SpanExportResult

            if code == 0:
                return SpanExportResult.SUCCESS
            return SpanExportResult.FAILURE
        except Exception:
            # Fallback for environments without OTel installed at runtime
            return {"code": code, "error": error}


# --- helpers ---

STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"


def _ns_to_iso(nanoseconds: int) -> str:
    from datetime import datetime, timezone

    seconds = nanoseconds // 1_000_000_000
    ns = nanoseconds % 1_000_000_000
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    # ISO format with nanoseconds
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{ns:09d}Z"


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()
