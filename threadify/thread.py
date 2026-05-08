from __future__ import annotations

import asyncio
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from threadify.notification import Notification
    from threadify.step import ThreadStep

from threadify.models import (
    ACTION_ADD_REFS,
    ACTION_CLOSE_THREAD,
    ACTION_INVITE_PARTY,
    ACTION_THREAD_END,
    DEFAULT_WAIT_TIMEOUT,
    FIELD_ACCESS_LEVEL,
    FIELD_ACTION,
    FIELD_CANCELLED_AT,
    FIELD_CLOSED_AT,
    FIELD_COMPLETED_AT,
    FIELD_EXPIRES_AT,
    FIELD_EXPIRES_IN,
    FIELD_MESSAGE,
    FIELD_REASON,
    FIELD_REFS,
    FIELD_ROLE,
    FIELD_STATUS,
    FIELD_THREAD_ID,
    FIELD_THREAD_STATUS,
    FIELD_THREAD_TOKEN,
    STATUS_CANCELLED,
    STATUS_COMPLETED,
    STATUS_SUCCESS,
    InviteOptions,
    InviteResponse,
    ThreadEndResponse,
    WaitOptions,
    first_non_empty,
    require_non_empty,
)

UUID_REGEX = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)


class ThreadInstance:
    """Represents an active thread on the Threadify Engine.

    Usage::

        thread = await conn.start("Order-123")
        step = thread.step("order_placed")
        result = await step.add_context({"orderId": "ORD-123"}).success("Order received")
        await thread.complete("All done")
    """

    def __init__(
        self,
        conn: Any,  # Connection (forward ref to avoid circular import)
        thread_id: str,
        contract_id: str = "",
        role: str = "",
        access_level: str = "",
        refs: dict[str, str] | None = None,
    ):
        self._conn = conn
        self.thread_id = thread_id
        self.contract_id = contract_id
        self.role = role
        self.access_level = access_level
        self.refs: dict[str, str] = refs or {}

        self._steps: dict[str, Any] = {}
        self._pending_waits: dict[str, _PendingWait] = {}

    def step(self, step_name: str) -> ThreadStep:
        """Create a new step builder for this thread.

        Args:
            step_name: Unique name for the step.

        Returns:
            A ThreadStep builder.
        """
        from threadify.step import ThreadStep

        s = ThreadStep(step_name, self, first_non_empty(self._conn.service_name))
        if not step_name or not step_name.strip():
            s._error = ValueError("step_name must be a non-empty string")

        self._steps[step_name] = s
        return s

    async def invite_party(self, options: InviteOptions) -> InviteResponse:
        """Create an invitation token for an external party.

        Args:
            options: Invitation configuration (role is required).

        Returns:
            InviteResponse with the token and metadata.
        """
        require_non_empty("role", options.role)

        msg = {
            FIELD_ACTION: ACTION_INVITE_PARTY,
            FIELD_THREAD_ID: self.thread_id,
            FIELD_ROLE: options.role,
            FIELD_ACCESS_LEVEL: options.access_level or "external",
            FIELD_EXPIRES_IN: options.expires_in or "24h",
        }

        await self._send(msg)

        resp = await self._conn._wait_response(lambda m: m.get(FIELD_ACTION) == ACTION_INVITE_PARTY)

        if resp.get(FIELD_STATUS) != STATUS_SUCCESS:
            raise RuntimeError(resp.get(FIELD_MESSAGE, "failed to create invitation token"))

        return InviteResponse(
            token=resp.get(FIELD_THREAD_TOKEN, ""),
            thread_id=self.thread_id,
            role=resp.get(FIELD_ROLE, ""),
            access_level=resp.get(FIELD_ACCESS_LEVEL, ""),
            expires_at=resp.get(FIELD_EXPIRES_AT, ""),
        )

    async def wait_for(
        self,
        step_name: str,
        options: WaitOptions | None = None,
    ) -> Notification:
        """Block until a notification arrives for the given step.

        Args:
            step_name: Step to wait for.
            options: WaitOptions with timeout and status filters.

        Returns:
            The matching Notification.

        Raises:
            asyncio.TimeoutError: If the wait times out.
        """
        require_non_empty("step_name", step_name)

        timeout = DEFAULT_WAIT_TIMEOUT
        statuses: list[str] = []
        if options:
            if options.timeout > 0:
                timeout = options.timeout
            statuses = options.statuses

        fut: asyncio.Future[Notification] = asyncio.get_event_loop().create_future()
        pw = _PendingWait(future=fut, statuses=statuses)
        self._pending_waits[step_name] = pw

        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError as err:
            raise asyncio.TimeoutError(
                f"Timeout waiting for step: {step_name} ({timeout}s)"
            ) from err
        finally:
            self._pending_waits.pop(step_name, None)

    async def add_refs(self, refs: dict[str, str]) -> None:
        """Add external references to this thread.

        Args:
            refs: Key-value pairs of references.
        """
        if not refs:
            raise ValueError("refs must be a non-empty dict")

        msg = {
            FIELD_ACTION: ACTION_ADD_REFS,
            FIELD_THREAD_ID: self.thread_id,
            FIELD_REFS: refs,
        }

        await self._send(msg)

        resp = await self._conn._wait_response(lambda m: m.get(FIELD_ACTION) == ACTION_ADD_REFS)

        if resp.get(FIELD_STATUS) != STATUS_SUCCESS:
            raise RuntimeError(resp.get(FIELD_MESSAGE, "failed to add refs"))

        self.refs.update(refs)

    async def link_thread(self, thread_id: str, relationship: str = "parent") -> None:
        """Link this thread to another thread via a reference.

        Args:
            thread_id: UUID of the thread to link.
            relationship: Relationship type (default: "parent").
        """
        require_non_empty("thread_id", thread_id)
        if not UUID_REGEX.match(thread_id):
            raise ValueError("Invalid thread ID format")

        ref_key = f"linkedThread:{relationship}"
        await self.add_refs({ref_key: thread_id})

    async def end(
        self, status: str = STATUS_CANCELLED, reason: str | dict[str, Any] = ""
    ) -> ThreadEndResponse:
        """End the thread with the given status.

        Args:
            status: Thread end status ("cancelled", "completed").
            reason: Optional reason message (string) or data dict.
        """
        return await self._end_thread(status, reason)

    async def close(self, reason: str | dict[str, Any] = "") -> ThreadEndResponse:
        """Close the thread (convenience for end with 'cancelled')."""
        return await self._end_thread(STATUS_CANCELLED, reason)

    async def complete(self, reason: str | dict[str, Any] = "") -> ThreadEndResponse:
        """Complete the thread (convenience for end with 'completed')."""
        return await self._end_thread(STATUS_COMPLETED, reason)

    async def _end_thread(self, status: str, reason: str | dict[str, Any]) -> ThreadEndResponse:
        msg: dict[str, Any] = {
            FIELD_ACTION: ACTION_THREAD_END,
            FIELD_THREAD_ID: self.thread_id,
            FIELD_STATUS: status,
        }
        if isinstance(reason, str) and reason:
            msg[FIELD_REASON] = reason
        elif isinstance(reason, dict) and reason:
            msg.update(reason)

        await self._send(msg)

        resp = await self._conn._wait_response(
            lambda m: m.get(FIELD_ACTION) in (ACTION_THREAD_END, ACTION_CLOSE_THREAD)
        )

        if resp.get(FIELD_STATUS) != STATUS_SUCCESS:
            raise RuntimeError(resp.get(FIELD_MESSAGE, "failed to end thread"))

        self._cleanup()

        import datetime as dt

        ended_at = first_non_empty(
            resp.get(FIELD_CLOSED_AT, ""),
            resp.get(FIELD_COMPLETED_AT, ""),
            resp.get(FIELD_CANCELLED_AT, ""),
            dt.datetime.now(dt.timezone.utc).isoformat(),
        )

        return ThreadEndResponse(
            thread_id=self.thread_id,
            status=resp.get(FIELD_THREAD_STATUS, ""),
            ended_at=ended_at,
            message=resp.get(FIELD_MESSAGE, ""),
        )

    async def _send(self, msg: dict[str, Any]) -> None:
        await self._conn._send(msg)

    def _handle_notification(self, notif: Notification) -> None:
        """Route notification to pending wait_for calls."""
        pw = self._pending_waits.get(notif.step_name)
        if pw is None:
            return

        # Check status filter.
        if pw.statuses and notif.step_status not in pw.statuses:
            return

        # Deliver notification.
        if not pw.future.done():
            pw.future.set_result(notif)

    def get_step(self, step_name: str) -> Any | None:
        """Return the step instance for the given step name, if any."""
        return self._steps.get(step_name)

    def get_all_steps(self) -> list[Any]:
        """Return all recorded steps for this thread."""
        return list(self._steps.values())

    def get_thread_id(self) -> str:
        """Return the thread ID."""
        return self.thread_id

    def get_contract_id(self) -> str:
        """Return the contract ID (or empty string)."""
        return self.contract_id

    def create_span_exporter(self, options: dict[str, Any] | None = None) -> Any:
        """Create an OpenTelemetry SpanExporter wired to this thread.

        Args:
            options: Optional configuration dict. Supported keys:
                - ``refs``: list of attribute keys to map to Threadify refs.

        Returns:
            A :class:`~threadify.otel_exporter.ThreadifySpanExporter` instance.
        """
        from threadify.otel_exporter import ThreadifySpanExporter

        return ThreadifySpanExporter(self._conn, options or {})

    def _cleanup(self) -> None:
        """Cancel pending waits and unregister from connection."""
        for pw in self._pending_waits.values():
            if not pw.future.done():
                pw.future.cancel()
        self._pending_waits.clear()
        self._conn._remove_thread(self.thread_id)


class _PendingWait:
    """Internal state for a pending wait_for call."""

    def __init__(self, future: asyncio.Future, statuses: list[str]):
        self.future = future
        self.statuses = statuses
