from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from threadify.models import (
    FIELD_CONTRACT_NAME,
    FIELD_DETAILS,
    FIELD_MESSAGE,
    FIELD_NOTIFICATION_ID,
    FIELD_NOTIFICATION_TYPE,
    FIELD_OWNER_ID,
    FIELD_SEVERITY,
    FIELD_SOURCE,
    FIELD_STATUS,
    FIELD_STEP_NAME,
    FIELD_STEP_STATUS,
    FIELD_THREAD_ID,
    FIELD_TIMESTAMP,
    FIELD_VIOLATION_TYPE,
    SEVERITY_CRITICAL,
    SEVERITY_INFO,
    SEVERITY_WARNING,
    STATUS_ERROR,
    STATUS_FAILED,
    STATUS_PASSED,
    STATUS_SUCCESS,
    STATUS_VIOLATED,
)


class Notification:
    """Wraps a real-time notification from the Threadify Engine.

    Provides helper methods to inspect severity and status,
    and an :meth:`ack` method to acknowledge receipt.

    Usage::

        def handler(notif):
            if notif.is_violated and notif.is_critical:
                print(f"Critical: {notif.message}")
                notif.ack()
    """

    def __init__(
        self,
        data: dict[str, Any],
        connection: Any,
        ack_token: str = "",
    ):
        self.notification_id: str = data.get(FIELD_NOTIFICATION_ID, "")
        self.thread_id: str = data.get(FIELD_THREAD_ID, "")
        self.step_id: str = data.get("stepId", "")
        self.step_name: str = data.get(FIELD_STEP_NAME, "")
        self.contract_name: str = data.get(FIELD_CONTRACT_NAME, "")
        self.status: str = data.get(FIELD_STATUS, "")
        self.step_status: str = data.get(FIELD_STEP_STATUS, "")
        self.severity: str = data.get(FIELD_SEVERITY, "")
        self.message: str = data.get(FIELD_MESSAGE, "")
        self.details: dict[str, Any] = data.get(FIELD_DETAILS) or {}
        self.violation_type: str = data.get(FIELD_VIOLATION_TYPE, "")
        self.owner_id: str = data.get(FIELD_OWNER_ID, "")
        self.source: str = data.get(FIELD_SOURCE, "")
        self.notification_type: str = data.get(FIELD_NOTIFICATION_TYPE, "")

        ts_str = data.get(FIELD_TIMESTAMP, "")
        try:
            self.timestamp = (
                datetime.fromisoformat(ts_str) if ts_str else datetime.now(timezone.utc)
            )
        except (ValueError, TypeError):
            self.timestamp = datetime.now(timezone.utc)

        self._ack_token = ack_token
        self._connection = connection
        self._acknowledged = False

    def ack(self) -> None:
        """Acknowledge this notification so it won't be redelivered.

        ACK is idempotent — safe to call multiple times.

        Raises:
            RuntimeError: If ack_token is missing.
        """
        if self._acknowledged:
            return

        if not self._ack_token:
            raise RuntimeError(
                f"Cannot ACK notification {self.notification_id}: ackToken is required"
            )

        self._acknowledged = True
        self._connection._send_ack(self.notification_id, self.thread_id, self._ack_token)

    # --- Status helpers ---

    @property
    def is_acknowledged(self) -> bool:
        return self._acknowledged

    @property
    def is_violated(self) -> bool:
        return self.status == STATUS_VIOLATED

    @property
    def is_passed(self) -> bool:
        return self.status == STATUS_PASSED

    @property
    def is_critical(self) -> bool:
        return self.severity == SEVERITY_CRITICAL

    @property
    def is_warning(self) -> bool:
        return self.severity == SEVERITY_WARNING

    @property
    def is_info(self) -> bool:
        return self.severity == SEVERITY_INFO

    @property
    def is_success(self) -> bool:
        return self.step_status == STATUS_SUCCESS

    @property
    def is_failed(self) -> bool:
        return self.step_status == STATUS_FAILED

    @property
    def is_error(self) -> bool:
        return self.step_status == STATUS_ERROR

    # --- Serialisation ---

    def __str__(self) -> str:
        sev = self.severity or "unknown"
        return f"[{sev}] {self.step_name}: {self.message}"

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dict representation for logging/serialisation."""
        return {
            "notificationId": self.notification_id,
            "threadId": self.thread_id,
            "stepId": self.step_id,
            "stepName": self.step_name,
            "contractName": self.contract_name,
            "status": self.status,
            "stepStatus": self.step_status,
            "severity": self.severity,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
            "violationType": self.violation_type,
            "ownerId": self.owner_id,
            "acknowledged": self._acknowledged,
        }

    def to_json(self) -> str:
        """Return JSON string representation."""
        return json.dumps(self.to_dict())
