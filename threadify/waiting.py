"""Correlated Engine permission and validation protocol (timeouts in seconds)."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from threadify.models import WaitOptions, require_non_empty


class ThreadifyError(RuntimeError):
    def __init__(self, code: str, message: str, **details: Any):
        super().__init__(message)
        self.code = code
        for key, value in details.items():
            setattr(self, key, value)


@dataclass
class WaitResult:
    decision: str
    thread_id: str = ""
    step_name: str = ""
    step_id: str = ""
    invocation_id: str = ""
    message: str = ""
    violations: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class PermissionGrant(WaitResult):
    _thread: Any = field(default=None, repr=False, compare=False)

    async def cancel(self) -> WaitResult:
        response = await self._thread._conn._request(
            {
                "action": "waitFor",
                "threadId": self.thread_id,
                "stepName": self.step_name,
                "invocationId": self.invocation_id,
                "cancel": True,
            }
        )
        # A cancelled permission is the expected successful result here.
        if (
            response.get("decision") != "cancelled"
            or response.get("invocationId") != self.invocation_id
        ):
            raise ThreadifyError(
                "THREADIFY_PERMISSION_DENIED",
                "Engine did not cancel this invocation",
                response=response,
            )
        current = self._thread._invocation_grants.get(self.step_name)
        if current is self:
            self._thread._invocation_grants.pop(self.step_name, None)
        return decode_result(response)


def validate_timeout(timeout: float) -> None:
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
        or timeout > 300
    ):
        raise ValueError("timeout must be greater than zero and at most 300 seconds")


def decode_result(response: dict[str, Any]) -> WaitResult:
    return WaitResult(
        decision=response.get("decision", ""),
        thread_id=response.get("threadId", ""),
        step_name=response.get("stepName", ""),
        step_id=response.get("stepId", ""),
        invocation_id=response.get("invocationId", ""),
        message=response.get("message", ""),
        violations=response.get("violations") or [],
    )


def checked_result(response: dict[str, Any] | None) -> WaitResult:
    if (
        not isinstance(response, dict)
        or not response.get("decision")
        or response["decision"] == "pending"
    ):
        raise ThreadifyError(
            "THREADIFY_SYNC_WAIT_UNSUPPORTED",
            "Engine did not return a final synchronous wait result",
        )
    result = decode_result(response)
    if result.decision in ("timed_out", "cancelled"):
        code = (
            "THREADIFY_WAIT_TIMEOUT"
            if result.decision == "timed_out"
            else "THREADIFY_WAIT_CANCELLED"
        )
        raise ThreadifyError(
            code,
            result.message or "Wait ended",
            step_id=result.step_id,
            invocation_id=result.invocation_id,
        )
    return result


def checked_validation(response: dict[str, Any] | None, step_id: str) -> WaitResult:
    try:
        result = checked_result(response)
        if result.step_id != step_id:
            raise ThreadifyError(
                "THREADIFY_INVALID_WAIT_RESPONSE", "Engine returned a different event"
            )
        if result.decision != "passed":
            code = (
                "THREADIFY_VALIDATION_VIOLATED"
                if result.decision == "violated"
                else "THREADIFY_VALIDATION_UNAVAILABLE"
            )
            raise ThreadifyError(
                code, result.message or "Validation unavailable", validation=result
            )
        return result
    except ThreadifyError as exc:
        exc.step_id = step_id
        raise


async def wait_for_permission(
    thread: Any, step_name: str, options: WaitOptions | None
) -> PermissionGrant:
    require_non_empty("step_name", step_name)
    options = options or WaitOptions(timeout=10)
    validate_timeout(options.timeout)
    invocation_id = options.invocation_id or str(uuid4())
    if not isinstance(invocation_id, str) or not invocation_id.strip():
        raise ValueError("invocation_id must be a non-empty string")
    try:
        response = await thread._conn._request(
            {
                "action": "waitFor",
                "threadId": thread.thread_id,
                "stepName": step_name,
                "invocationId": invocation_id,
                "await": True,
                "timeoutMs": math.ceil(options.timeout * 1000),
            },
            options.timeout,
        )
        result = checked_result(response)
        if result.decision != "allowed":
            raise ThreadifyError(
                "THREADIFY_PERMISSION_DENIED",
                result.message or "Permission denied",
                decision=result,
            )
        if result.invocation_id != invocation_id:
            raise ThreadifyError(
                "THREADIFY_INVALID_WAIT_RESPONSE", "Engine returned a different invocation"
            )
        grant = PermissionGrant(**vars(result), _thread=thread)
        thread._invocation_grants[step_name] = grant
        return grant
    except Exception as exc:
        exc.invocation_id = invocation_id
        exc.step_name = step_name
        raise


async def wait_for_validation(
    thread: Any, step_name: str, step_id: str, options: WaitOptions | None
) -> WaitResult:
    require_non_empty("step_name", step_name)
    require_non_empty("step_id", step_id)
    timeout = options.timeout if options else 10
    validate_timeout(timeout)
    try:
        response = await thread._conn._request(
            {
                "action": "waitFor",
                "threadId": thread.thread_id,
                "stepName": step_name,
                "stepId": step_id,
                "await": True,
                "timeoutMs": math.ceil(timeout * 1000),
            },
            timeout,
        )
        return checked_validation(response, step_id)
    except Exception as exc:
        exc.step_id = step_id
        raise


def http_error(status: int, message: str) -> ThreadifyError:
    code = "THREADIFY_HTTP_ERROR"
    if status == 429:
        code = "THREADIFY_ALLOWANCE_EXCEEDED"
    elif status == 503:
        code = "THREADIFY_LICENSE_UNAVAILABLE"
    return ThreadifyError(code, message, status=status)
