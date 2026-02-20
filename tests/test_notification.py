from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from threadify.notification import Notification


def _make_connection():
    conn = MagicMock()
    conn._send_ack = MagicMock()
    return conn


class TestNotificationConstruction:
    def test_basic_fields(self):
        conn = _make_connection()
        data = {
            "notificationId": "n-001",
            "threadId": "t-123",
            "stepId": "s-456",
            "stepName": "order_placed",
            "contractName": "order_flow",
            "status": "violated",
            "stepStatus": "success",
            "severity": "critical",
            "message": "Missing amount",
            "source": "validation",
            "notificationType": "validation.violated",
        }
        notif = Notification(data, conn, "ack-tok")

        assert notif.notification_id == "n-001"
        assert notif.thread_id == "t-123"
        assert notif.step_name == "order_placed"
        assert notif.contract_name == "order_flow"
        assert notif.severity == "critical"
        assert notif.source == "validation"
        assert notif.notification_type == "validation.violated"

    def test_valid_timestamp(self):
        conn = _make_connection()
        notif = Notification(
            {"notificationId": "n", "timestamp": "2026-02-18T12:00:00+00:00"},
            conn,
        )
        expected = datetime(2026, 2, 18, 12, 0, 0, tzinfo=timezone.utc)
        assert notif.timestamp == expected

    def test_invalid_timestamp_fallback(self):
        conn = _make_connection()
        before = datetime.now(timezone.utc)
        notif = Notification({"notificationId": "n", "timestamp": "not-a-date"}, conn)
        after = datetime.now(timezone.utc)
        assert before <= notif.timestamp <= after

    def test_missing_timestamp_fallback(self):
        conn = _make_connection()
        before = datetime.now(timezone.utc)
        notif = Notification({"notificationId": "n"}, conn)
        after = datetime.now(timezone.utc)
        assert before <= notif.timestamp <= after


class TestAck:
    def test_success(self):
        conn = _make_connection()
        notif = Notification({"notificationId": "n-ack-1", "threadId": "t-1"}, conn, "ack-tok-1")
        notif.ack()

        assert notif.is_acknowledged
        conn._send_ack.assert_called_once_with("n-ack-1", "t-1", "ack-tok-1")

    def test_idempotent(self):
        conn = _make_connection()
        notif = Notification({"notificationId": "n-ack-2", "threadId": "t-1"}, conn, "ack-tok-2")
        notif.ack()
        notif.ack()  # Second call should be no-op.

        assert conn._send_ack.call_count == 1

    def test_missing_token_raises(self):
        conn = _make_connection()
        notif = Notification({"notificationId": "n-no-tok"}, conn, "")
        with pytest.raises(RuntimeError, match="ackToken"):
            notif.ack()


class TestStatusHelpers:
    @pytest.mark.parametrize(
        "status,is_violated,is_passed",
        [
            ("violated", True, False),
            ("passed", False, True),
            ("none", False, False),
        ],
    )
    def test_status(self, status, is_violated, is_passed):
        conn = _make_connection()
        notif = Notification({"notificationId": "n", "status": status}, conn)
        assert notif.is_violated == is_violated
        assert notif.is_passed == is_passed


class TestSeverityHelpers:
    @pytest.mark.parametrize(
        "severity,is_critical,is_warning,is_info",
        [
            ("critical", True, False, False),
            ("warning", False, True, False),
            ("info", False, False, True),
            ("unknown", False, False, False),
        ],
    )
    def test_severity(self, severity, is_critical, is_warning, is_info):
        conn = _make_connection()
        notif = Notification({"notificationId": "n", "severity": severity}, conn)
        assert notif.is_critical == is_critical
        assert notif.is_warning == is_warning
        assert notif.is_info == is_info


class TestStepStatusHelpers:
    @pytest.mark.parametrize(
        "step_status,is_success,is_failed,is_error",
        [
            ("success", True, False, False),
            ("failed", False, True, False),
            ("error", False, False, True),
        ],
    )
    def test_step_status(self, step_status, is_success, is_failed, is_error):
        conn = _make_connection()
        notif = Notification({"notificationId": "n", "stepStatus": step_status}, conn)
        assert notif.is_success == is_success
        assert notif.is_failed == is_failed
        assert notif.is_error == is_error


class TestSerialisation:
    def test_str(self):
        conn = _make_connection()
        notif = Notification(
            {
                "notificationId": "n",
                "stepName": "order_placed",
                "severity": "critical",
                "message": "Missing",
            },
            conn,
        )
        s = str(notif)
        assert "critical" in s
        assert "order_placed" in s

    def test_to_dict(self):
        conn = _make_connection()
        notif = Notification(
            {"notificationId": "n-d", "threadId": "t-1", "stepName": "s"},
            conn,
            "tok",
        )
        d = notif.to_dict()
        assert d["notificationId"] == "n-d"
        assert d["threadId"] == "t-1"
        assert d["acknowledged"] is False

        notif.ack()
        d2 = notif.to_dict()
        assert d2["acknowledged"] is True

    def test_to_json(self):
        conn = _make_connection()
        notif = Notification(
            {"notificationId": "n-j", "severity": "info", "message": "ok"},
            conn,
        )
        j = notif.to_json()
        assert '"notificationId": "n-j"' in j or '"notificationId":"n-j"' in j
