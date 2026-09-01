import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from threadify.models import (
    ACTION_ADD_REFS,
    ACTION_INVITE_PARTY,
    ACTION_THREAD_END,
    STATUS_CANCELLED,
    STATUS_COMPLETED,
    STATUS_SUCCESS,
    InviteOptions,
    WaitOptions,
)
from threadify.notification import Notification
from threadify.thread import ThreadInstance


def _make_mock_conn():
    conn = AsyncMock()
    conn.service_name = "test-service"
    conn._send = AsyncMock()
    conn._wait_response = AsyncMock()
    conn._remove_thread = MagicMock()
    return conn


@pytest.mark.asyncio
async def test_invite_party():
    conn = _make_mock_conn()
    conn._wait_response.return_value = {
        "action": ACTION_INVITE_PARTY,
        "status": STATUS_SUCCESS,
        "threadToken": "token-123",
        "role": "supplier",
        "accessLevel": "external",
        "expiresAt": "2026-02-20T12:00:00Z",
    }

    thread = ThreadInstance(conn, "thread-123")
    options = InviteOptions(role="supplier", expires_in="1h")

    resp = await thread.invite_party(options)

    assert resp.token == "token-123"
    assert resp.role == "supplier"
    conn._send.assert_called_once()
    sent_msg = conn._send.call_args.args[0]
    assert sent_msg["action"] == ACTION_INVITE_PARTY
    assert sent_msg["role"] == "supplier"


@pytest.mark.asyncio
async def test_wait_for():
    conn = _make_mock_conn()
    thread = ThreadInstance(conn, "thread-123")

    # Simulate notification after a small delay
    async def simulate_notif():
        await asyncio.sleep(0.05)
        notif_data = {
            "notificationId": "n-1",
            "threadId": "thread-123",
            "stepName": "order_placed",
            "source": "execution",
            "notificationType": "step.success",
            "stepStatus": "success",
        }
        notif = Notification(data=notif_data, connection=conn)
        thread._handle_notification(notif)

    asyncio.create_task(simulate_notif())

    notif = await thread.wait_for("order_placed", WaitOptions(timeout=1))
    assert notif.step_name == "order_placed"
    assert notif.step_status == "success"


@pytest.mark.asyncio
async def test_add_refs():
    conn = _make_mock_conn()
    conn._wait_response.return_value = {"action": ACTION_ADD_REFS, "status": STATUS_SUCCESS}

    thread = ThreadInstance(conn, "thread-123")
    refs = {"orderId": "ORD-999"}

    await thread.add_refs(refs)

    assert thread.refs["orderId"] == "ORD-999"
    conn._send.assert_called_once()
    sent_msg = conn._send.call_args.args[0]
    assert sent_msg["action"] == ACTION_ADD_REFS
    assert sent_msg["refs"] == refs


@pytest.mark.asyncio
async def test_link_thread():
    conn = _make_mock_conn()
    conn._wait_response.return_value = {"action": ACTION_ADD_REFS, "status": STATUS_SUCCESS}

    thread = ThreadInstance(conn, "thread-123")
    target_id = "00000000-0000-0000-0000-000000000001"

    await thread.link_thread(target_id, relationship="child")

    assert thread.refs["linkedThread:child"] == target_id
    conn._send.assert_called_once()


@pytest.mark.asyncio
async def test_end_thread_complete():
    conn = _make_mock_conn()
    conn._wait_response.return_value = {
        "action": ACTION_THREAD_END,
        "status": STATUS_SUCCESS,
        "threadStatus": STATUS_COMPLETED,
        "completedAt": "2026-02-19T19:00:00Z",
    }

    thread = ThreadInstance(conn, "thread-123")
    resp = await thread.complete("mission finished")

    assert resp.status == STATUS_COMPLETED
    conn._send.assert_called_once()
    sent_msg = conn._send.call_args.args[0]
    assert sent_msg["status"] == STATUS_COMPLETED
    assert sent_msg["reason"] == "mission finished"
    conn._remove_thread.assert_called_once_with("thread-123")


@pytest.mark.asyncio
async def test_end_thread_close():
    conn = _make_mock_conn()
    conn._wait_response.return_value = {
        "action": ACTION_THREAD_END,
        "status": STATUS_SUCCESS,
        "threadStatus": STATUS_CANCELLED,
        "closedAt": "2026-02-19T19:00:00Z",
    }

    thread = ThreadInstance(conn, "thread-123")
    resp = await thread.close("abort")

    assert resp.status == STATUS_CANCELLED
    sent_msg = conn._send.call_args.args[0]
    assert sent_msg["status"] == STATUS_CANCELLED
    assert sent_msg["reason"] == "abort"
