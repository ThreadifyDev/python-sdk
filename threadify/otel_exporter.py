from __future__ import annotations

import asyncio
import logging
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
        thread = await conn.start("Order-123")

        exporter = thread.create_span_exporter(options={"refs": ["orderId"]})

        provider = TracerProvider()
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
    """

    def __init__(self, connection: Connection, options: dict[str, Any] | None = None):
        _require_otel()
        self._connection = connection
        self._options = options or {}

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

        # Capture the event loop so we can schedule coroutines from sync export().
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None

    def export(
        self, spans: list[ReadableSpan], timeout_millis: float = 30000
    ) -> Any:
        """Export a batch of spans.

        Called by the OpenTelemetry ``BatchSpanProcessor`` (typically from a
        worker thread). We schedule the async work on the captured event loop.
        """
        if not self._connection.is_connected:
            return self._make_result(1, "Threadify connection is not open")

        if self._loop is None or self._loop.is_closed():
            return self._make_result(1, "No running event loop available")

        asyncio.run_coroutine_threadsafe(self._process_all(spans), self._loop)
        return self._make_result(0)

    def force_flush(self, timeout_millis: float = 30000) -> bool:
        """No-op — spans are sent immediately."""
        return True

    def shutdown(self) -> None:
        """No-op — connection lifecycle is managed externally."""
        return None

    # --- internals ---

    async def _process_all(self, spans: list[ReadableSpan]) -> None:
        for span in spans:
            if self._should_drop(span.name):
                continue
            await self._process_span(span)

    async def _process_span(self, span: ReadableSpan) -> None:
        try:
            await self._do_process_span(span)
        except Exception:
            logger.exception("Failed to process span")

    async def _do_process_span(self, span: ReadableSpan) -> None:
        ctx = span.get_span_context()
        trace_id = format(ctx.trace_id, "032x")
        span_id = format(ctx.span_id, "016x")
        thread = await self._get_or_start_thread(span, trace_id)

        # Step name
        step_name = self._span_attr(span, "threadify.step_name") or span.name
        step = thread.step(step_name)

        # Separate attributes into context / refs
        context: dict[str, str] = {}
        refs: dict[str, str] = {
            "otel_trace_id": trace_id,
            "otel_span_id": span_id,
        }

        for key, value in span.attributes.items():
            # Skip internal threadify directives
            if key in {
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
                refs[ref_key] = str_value
            elif key.startswith("threadify.context."):
                context[key.replace("threadify.context.", "")] = str_value
            else:
                context[key] = str_value

        if context:
            step.add_context(context)
        if refs:
            step.add_refs(refs)

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
        if not parent_span_id:
            if target_status == STATUS_SUCCESS:
                await thread.complete("Root span completed successfully")
            else:
                await thread.close("Root span failed")
            # Clean up the trace map since the trace is finished
            self._trace_threads.pop(trace_id, None)

    async def _get_or_start_thread(self, span: ReadableSpan, trace_id: str) -> Any:
        """Get or create a ThreadInstance for this trace."""
        from threadify.thread import ThreadInstance

        if trace_id not in self._trace_threads:
            fut: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
            self._trace_threads[trace_id] = fut

            try:
                existing_thread_id = self._span_attr(span, "threadify.thread_id")
                if existing_thread_id:
                    role = self._span_attr(span, "threadify.role") or "participant"
                    thread = await self._connection.join(existing_thread_id, role)
                else:
                    contract_name = self._span_attr(span, "threadify.contract")
                    label = self._span_attr(span, "threadify.label") or span.name
                    service_name = (
                        self._span_attr(span, "threadify.service")
                        or self._connection.service_name
                    )
                    role = self._span_attr(span, "threadify.role") or "participant"

                    # Try to find an existing thread via GraphQL
                    try:
                        archived = await self._connection.get_thread_by_ref(
                            "otel_trace_id", trace_id
                        )
                        if archived:
                            logger.debug(
                                "Found existing thread %s via GraphQL, joining...",
                                archived.id,
                            )
                            thread = await self._connection.join(archived.id, role)
                            fut.set_result(thread)
                            return thread
                    except Exception:
                        pass

                    tags = self._span_attr_list(span, "threadify.tags")
                    thread = await self._connection.start(
                        label=label,
                        contract_name=contract_name or "",
                        service_name=service_name,
                        tags=tags,
                    )
                fut.set_result(thread)
            except Exception as exc:
                fut.set_exception(exc)
                raise

            # Memory-leak safety: remove after 10 minutes
            asyncio.get_event_loop().call_later(
                600, self._trace_threads.pop, trace_id, None
            )

        return await self._trace_threads[trace_id]

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
