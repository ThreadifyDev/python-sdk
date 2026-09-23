"""Exercise keyed acquisition through the real request-ID WebSocket dispatcher."""

import asyncio
import json
import unittest

from threadify import ThreadifyError
from threadify.connection import Connection


class Socket:
    def __init__(self):
        self.incoming = asyncio.Queue()
        self.sent = asyncio.Queue()

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self.incoming.get()
        if item is None:
            raise StopAsyncIteration
        return json.dumps(item)

    async def send(self, value):
        await self.sent.put(json.loads(value))

    async def close(self):
        await self.incoming.put(None)

    async def reply(self, request, **result):
        await self.incoming.put(
            {
                "status": "success",
                "action": request["action"],
                "requestId": request["requestId"],
                "threadKey": request.get("threadKey"),
                "threadId": "stored-id",
                **result,
            }
        )


class ThreadKeyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ws = Socket()
        self.conn = Connection(self.ws, "key", "agent-service", "http://unused/graphql")
        self.tasks = []

    async def asyncTearDown(self):
        await self.ws.close()
        await self.conn._listener_task
        self.conn._heartbeat_task.cancel()
        for task in self.tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(self.conn._heartbeat_task, *self.tasks, return_exceptions=True)

    def acquire(self, key, options=None):
        task = asyncio.create_task(self.conn.thread(key, options))
        self.tasks.append(task)
        return task

    async def request(self):
        return await asyncio.wait_for(self.ws.sent.get(), 1)

    async def test_creation_and_resume_load_stored_defaults_and_pinned_contract(self):
        options = {
            "label": "Session",
            "contract": "agent",
            "refs": {"customer": "123"},
            "tags": ["priority"],
            "service_name": "worker",
            "role": "agent",
        }
        first = self.acquire(" session:123 ", options)
        request = await self.request()
        self.assertEqual(request["action"], "thread")
        self.assertEqual(request["threadKey"], "session:123")
        self.assertEqual(request["contractName"], "agent")
        self.assertEqual(request["serviceName"], "worker")
        self.assertEqual(request["role"], "agent")
        metadata = {
            "label": "Session",
            "contractId": "contract-id",
            "contractName": "agent",
            "contractVersion": 4,
            "refs": {"customer": "123"},
            "tags": ["priority"],
        }
        await self.ws.reply(request, **metadata)
        thread = await first
        # Supply changed creation defaults on resume; Engine's persisted values win.
        resumed = self.acquire("session:123", {"label": "Changed", "refs": {"customer": "456"}})
        request = await self.request()
        self.assertNotIn("contractName", request)
        self.assertEqual(request["serviceName"], "agent-service")
        await self.ws.reply(request, **metadata)
        self.assertIs(await resumed, thread)
        self.assertEqual((thread.thread_key, thread.label), ("session:123", "Session"))
        self.assertEqual(
            (thread.contract_id, thread.contract_name, thread.contract_version),
            ("contract-id", "agent", 4),
        )
        self.assertEqual(thread.refs, {"customer": "123"})
        self.assertEqual(thread.tags, ["priority"])
        self.assertEqual(options["refs"], {"customer": "123"})
        # A separate connection/worker does not need locally cached metadata.
        self.conn._threads.clear()
        resumed = self.acquire("session:123")
        request = await self.request()
        self.assertNotIn("contractName", request)
        await self.ws.reply(request, **metadata)
        self.assertEqual((await resumed).contract_version, 4)

    async def test_concurrent_out_of_order_responses_match_only_request_id(self):
        first = self.acquire("first")
        second = self.acquire("second")
        req1, req2 = await self.request(), await self.request()
        self.assertNotEqual(req1["requestId"], req2["requestId"])
        await self.ws.reply({**req1, "requestId": "wrong"}, threadId="wrong")
        await self.ws.reply(req2, threadId="id-2")
        self.assertEqual((await second).thread_key, "second")
        self.assertFalse(first.done())
        await self.ws.reply(req1, threadId="id-1")
        self.assertEqual((await first).thread_id, "id-1")
        self.assertFalse(self.conn._requests)

    async def test_conflicting_and_closed_keys_never_fall_back_to_creation(self):
        for message in ("thread is closed", "contract conflicts with stored thread"):
            task = self.acquire("session", {"contract": "other"})
            request = await self.request()
            await self.ws.reply(request, status="error", message=message)
            with self.assertRaisesRegex(ThreadifyError, message):
                await task
            self.assertTrue(self.ws.sent.empty())
            self.assertFalse(self.conn._threads)
            self.assertFalse(self.conn._requests)

    async def test_validation_rejects_invalid_keys_and_options_before_sending(self):
        for key in (None, 42, "", "  ", "é" * 513):
            with self.subTest(key=repr(key)[:30]), self.assertRaises(ValueError):
                await self.conn.thread(key)
        for options in (
            [],
            {"contract": ""},
            {"label": " "},
            {"service_name": 3},
            {"refs": {"a": 3}},
            {"refs": {"": "a"}},
            {"tags": [""]},
            {"contract_name": "old-spelling"},
        ):
            with self.subTest(options=options), self.assertRaises((TypeError, ValueError)):
                await self.conn.thread("session", options)
        self.assertTrue(self.ws.sent.empty())

    async def test_missing_or_mismatched_engine_identity_rejected(self):
        for response in ({"threadId": ""}, {"threadKey": "other"}):
            task = self.acquire("session")
            await self.ws.reply(await self.request(), **response)
            with self.assertRaisesRegex(RuntimeError, "invalid thread identity"):
                await task
        self.assertFalse(self.conn._threads)

    async def test_disconnect_rejects_pending_acquisition_without_retry(self):
        task = self.acquire("session")
        await self.request()
        await self.ws.close()
        with self.assertRaises(ConnectionError):
            await task
        self.assertTrue(self.ws.sent.empty())
        self.assertFalse(self.conn._requests)

    async def test_trace_only_compatibility_loads_stored_contract_metadata(self):
        task = asyncio.create_task(self.conn.start(refs={"otel_trace_id": "trace-id"}))
        self.tasks.append(task)
        request = await self.request()
        self.assertEqual(request["action"], "startThread")
        await self.ws.incoming.put(
            {
                "action": "startThread",
                "status": "success",
                "threadId": "stored-id",
                "contractId": "contract-id",
                "contractName": "agent",
                "contractVersion": 4,
                "label": "Original",
                "refs": {"customer": "123"},
                "tags": ["stored"],
            }
        )
        thread = await task
        self.assertEqual(
            (thread.contract_id, thread.contract_name, thread.contract_version),
            ("contract-id", "agent", 4),
        )
        self.assertEqual(thread.label, "Original")
        self.assertEqual(thread.refs, {"customer": "123"})
        self.assertEqual(thread.tags, ["stored"])
