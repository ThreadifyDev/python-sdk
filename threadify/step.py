from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from threadify.thread import ThreadInstance

from threadify.models import (
    ACTION_RECORD_THREAD_EVENT,
    FIELD_ACTION,
    FIELD_CONTEXT,
    FIELD_FINISHED_AT,
    FIELD_IDEMPOTENCY_KEY,
    FIELD_IS_DUPLICATE,
    FIELD_MESSAGE,
    FIELD_REFS,
    FIELD_SERVICE_NAME,
    FIELD_STARTED_AT,
    FIELD_STATUS,
    FIELD_STEP_NAME,
    FIELD_SUB_STEPS,
    FIELD_THREAD_ID,
    FIELD_THREADIFY_METADATA,
    STATUS_ERROR,
    STATUS_FAILED,
    STATUS_IN_PROGRESS,
    STATUS_SUCCESS,
    StepResult,
    SubStepData,
    first_non_empty,
    now_iso,
)


class ThreadStep:
    """Fluent builder for recording step events.

    Usage::

        step = thread.step("order_placed")
        result = await (
            step
            .add_context({"orderId": "ORD-123", "amount": 99.99})
            .add_refs({"stripe_id": "pi_abc"})
            .success("Order placed!")
        )
    """

    def __init__(
        self,
        step_name: str,
        thread: ThreadInstance,
        service_name: str,
    ):
        self._step_name = step_name
        self._thread = thread
        self._service_name = service_name

        self._manual_idempotency_key: str = ""
        self._sub_steps: list[SubStepData] = []
        self._context: dict[str, str] = {}
        self._refs: dict[str, str] = {}
        self._metadata: dict[str, Any] | None = None
        self._error: Exception | None = None

        self._event: dict[str, Any] = {
            FIELD_ACTION: ACTION_RECORD_THREAD_EVENT,
            FIELD_THREAD_ID: thread.thread_id,
            FIELD_STEP_NAME: step_name,
            FIELD_STARTED_AT: now_iso(),
            FIELD_FINISHED_AT: None,
            FIELD_STATUS: STATUS_IN_PROGRESS,
            FIELD_SERVICE_NAME: service_name,
        }

    # --- Fluent builder methods ---

    def idempotency_key(self, key: str) -> ThreadStep:
        """Set a manual idempotency key for deduplication."""
        if self._error is not None:
            return self
        if not key or not key.strip():
            self._error = ValueError("idempotency key must be a non-empty string")
            return self
        self._manual_idempotency_key = key
        return self

    def add_context(self, data: dict[str, Any] | None) -> ThreadStep:
        """Add business context data to this step.

        All values are converted to strings to match the server schema.
        """
        if self._error is not None:
            return self
        if data:
            for k, v in data.items():
                self._context[k] = str(v)
        return self

    def add_private_context(self, data: dict[str, Any] | None) -> ThreadStep:
        """Add private context data (prefixed with 'private_')."""
        if self._error is not None:
            return self
        if data:
            for k, v in data.items():
                s = str(v)
                self._context[k] = s
                self._context[f"private_{k}"] = s
        return self

    def add_refs(self, refs: dict[str, str] | None) -> ThreadStep:
        """Add external system references."""
        if self._error is not None:
            return self
        if refs:
            self._refs.update(refs)
        return self

    def sub_step(
        self,
        name: str,
        data: dict[str, Any] | None = None,
        status: str = "success",
    ) -> ThreadStep:
        """Record a sub-step within this step.

        Args:
            name: Sub-step name.
            data: Optional payload data.
            status: Must be 'success' or 'failed'.
        """
        if self._error is not None:
            return self
        if not name or not name.strip():
            self._error = ValueError("sub-step name must be a non-empty string")
            return self
        if status not in (STATUS_SUCCESS, STATUS_FAILED):
            self._error = ValueError(
                f'sub-step status must be either "{STATUS_SUCCESS}" or "{STATUS_FAILED}"'
            )
            return self

        self._sub_steps.append(
            SubStepData(
                name=name,
                status=status,
                payload=data,
            )
        )
        return self

    # --- Status methods ---

    async def success(self, message_or_data: str | dict | None = None) -> StepResult:
        """Mark the step as successful and send it."""
        return await self._stop(STATUS_SUCCESS, message_or_data)

    async def failed(self, message_or_data: str | dict | None = None) -> StepResult:
        """Mark the step as failed and send it."""
        return await self._stop(STATUS_FAILED, message_or_data)

    async def error(self, message_or_data: str | dict | None = None) -> StepResult:
        """Mark the step as error and send it."""
        return await self._stop(STATUS_ERROR, message_or_data)

    async def _stop(self, status: str, message_or_data: str | dict | None = None) -> StepResult:
        """Finalise the step and send the event."""
        if self._error is not None:
            raise self._error

        self._event[FIELD_FINISHED_AT] = now_iso()
        self._event[FIELD_STATUS] = status
        self._event[FIELD_CONTEXT] = self._context
        self._event[FIELD_REFS] = self._refs

        # Handle optional message/data.
        if message_or_data is not None:
            if self._metadata is None:
                self._metadata = {}
            if isinstance(message_or_data, str) and message_or_data:
                self._metadata[FIELD_MESSAGE] = message_or_data
            elif isinstance(message_or_data, dict) and message_or_data:
                self._metadata.update(message_or_data)

        if self._metadata:
            self._event[FIELD_THREADIFY_METADATA] = self._metadata

        # Attach sub-steps.
        if self._sub_steps:
            self._event[FIELD_SUB_STEPS] = [
                {
                    "name": ss.name,
                    "status": ss.status,
                    "payload": ss.payload,
                    "recordedAt": ss.recorded_at,
                }
                for ss in self._sub_steps
            ]

        # Generate idempotency key.
        self._event[FIELD_IDEMPOTENCY_KEY] = self._generate_idempotency_key()

        # Send event.
        try:
            await self._send_event()
        except DuplicateStepError:
            return StepResult(
                step_name=self._step_name,
                thread_id=self._thread.thread_id,
                status=status,
                idempotency_key=self._event.get(FIELD_IDEMPOTENCY_KEY, ""),
                timestamp=first_non_empty(
                    self._event.get(FIELD_FINISHED_AT, ""),
                    self._event.get(FIELD_STARTED_AT, ""),
                ),
                duplicate=True,
            )

        return StepResult(
            step_name=self._step_name,
            thread_id=self._thread.thread_id,
            status=status,
            idempotency_key=self._event.get(FIELD_IDEMPOTENCY_KEY, ""),
            timestamp=first_non_empty(
                self._event.get(FIELD_FINISHED_AT, ""),
                self._event.get(FIELD_STARTED_AT, ""),
            ),
        )

    async def _send_event(self) -> dict[str, Any]:
        """Transmit the event and wait for a response."""
        if not self._thread.thread_id:
            raise RuntimeError("Thread not started")

        await self._thread._send(self._event)

        resp = await self._thread._conn._wait_response(
            lambda m: m.get(FIELD_ACTION) == ACTION_RECORD_THREAD_EVENT
        )

        if resp.get(FIELD_STATUS) != STATUS_SUCCESS:
            msg = resp.get(FIELD_MESSAGE, "failed to record step event")
            if resp.get(FIELD_IS_DUPLICATE):
                raise DuplicateStepError(msg)
            raise RuntimeError(msg)

        return resp

    def _generate_idempotency_key(self) -> str:
        """Generate an FNV-1a idempotency key from step name + context."""
        if self._manual_idempotency_key:
            return self._manual_idempotency_key

        # Build sorted JSON string of context.
        sorted_items = sorted(self._context.items())
        context_json = "{" + ",".join(f'"{k}":"{v}"' for k, v in sorted_items) + "}"

        input_str = self._step_name + context_json
        h = _fnv1a_32(input_str.encode("utf-8"))
        return f"{h:08x}"

    # --- Read-only accessors ---

    @property
    def step_name(self) -> str:
        return self._step_name

    @property
    def status(self) -> str:
        return self._event.get(FIELD_STATUS, STATUS_IN_PROGRESS)

    @property
    def context(self) -> dict[str, str]:
        return dict(self._context)

    def get_event_data(self) -> dict[str, Any]:
        """Return a copy of the current event data (for debugging)."""
        return dict(self._event)


class DuplicateStepError(Exception):
    """Raised when a duplicate step is detected."""

    pass


def is_duplicate_error(error: Exception) -> bool:
    return isinstance(error, DuplicateStepError)


def _fnv1a_32(data: bytes) -> int:
    """FNV-1a 32-bit hash — matches the JS SDK implementation."""
    h = 0x811C9DC5
    for byte in data:
        h ^= byte
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h
