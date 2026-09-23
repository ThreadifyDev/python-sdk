from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from threadify.data_retriever import ArchivedThread, DataRetriever
    from threadify.notification import Notification
    from threadify.thread import ThreadInstance

from threadify.models import (
    ACTION_ACK_NOTIFICATION,
    ACTION_CLOSE_CONNECTION,
    ACTION_HEARTBEAT,
    ACTION_JOIN_THREAD,
    ACTION_NOTIFICATION,
    ACTION_NOTIFICATION_BATCH,
    ACTION_START_THREAD,
    ACTION_SUBSCRIBE,
    ACTION_UNSUBSCRIBE,
    DEFAULT_PROCESSED_MAX_SIZE,
    FIELD_ACCESS_LEVEL,
    FIELD_ACK_TOKEN,
    FIELD_ACTION,
    FIELD_CONTRACT_NAME,
    FIELD_EVENT_TYPES,
    FIELD_MESSAGE,
    FIELD_NOTIFICATION,
    FIELD_NOTIFICATION_ID,
    FIELD_NOTIFICATION_ID_ACK,
    FIELD_NOTIFICATIONS,
    FIELD_PROCESSED,
    FIELD_REFS,
    FIELD_ROLE,
    FIELD_SERVICE_NAME,
    FIELD_STATUS,
    FIELD_STEP_NAME,
    FIELD_TAGS,
    FIELD_THREAD_ID,
    FIELD_THREAD_ID_ACK,
    FIELD_THREAD_TOKEN,
    STATUS_SUCCESS,
    RefQuery,
    ThreadOptions,
    first_non_empty,
    require_non_empty,
)

logger = logging.getLogger("threadify")

NotificationHandler = Callable[["Notification"], Any]


class Connection:
    def __init__(
        self,
        ws: Any,
        api_key: str,
        service_name: str,
        graphql_url: str,
        debug: bool = False,
        max_in_flight: int = 10,
        logger: logging.Logger | None = None,
    ):
        self._ws = ws
        self._api_key = api_key
        self._service_name = service_name
        self._graphql_url = graphql_url
        self._debug = debug
        self._max_in_flight = max_in_flight
        self._logger = logger or logging.getLogger("threadify")
        self._connected = True

        self._threads: dict[str, Any] = {}

        self._notification_handlers: dict[str, list[NotificationHandler]] = {}
        self._active_subscriptions: dict[str, list[str]] = {}
        self._processed_notifications: set[str] = set()
        self._processed_notifications_max_size: int = DEFAULT_PROCESSED_MAX_SIZE

        self._recv_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._response_wait_lock = asyncio.Lock()
        self._closed_event = asyncio.Event()
        self._requests: dict[str, asyncio.Future] = {}

        self._data_retriever: DataRetriever | None = None

        self._listener_task = asyncio.ensure_future(self._read_loop())
        self._heartbeat_task = asyncio.ensure_future(self._heartbeat_loop())

    @property
    def service_name(self) -> str:
        return self._service_name

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def _read_loop(self) -> None:
        try:
            async for raw in self._ws:
                try:
                    msg = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue

                action = msg.get(FIELD_ACTION, "")

                if action == ACTION_NOTIFICATION:
                    self._handle_notification(
                        msg.get(FIELD_NOTIFICATION, {}),
                        msg.get(FIELD_ACK_TOKEN, ""),
                    )
                elif action == ACTION_NOTIFICATION_BATCH:
                    for n in msg.get(FIELD_NOTIFICATIONS, []):
                        if isinstance(n, dict):
                            self._handle_notification(n, "")
                elif msg.get("requestId"):
                    pending = self._requests.get(msg["requestId"])
                    if pending is not None and not pending.done():
                        pending.set_result(msg)
                else:
                    await self._recv_queue.put(msg)
        except Exception as exc:
            self._logger.error(f"readLoop error: {exc}")
        finally:
            self._connected = False
            self._closed_event.set()
            for pending in self._requests.values():
                if not pending.done():
                    pending.set_exception(
                        ConnectionError(
                            "Threadify connection closed; operation outcome may be unknown"
                        )
                    )

    async def _heartbeat_loop(self) -> None:
        while self._connected:
            await asyncio.sleep(30)
            if not self._connected:
                break
            try:
                await self._send({FIELD_ACTION: ACTION_HEARTBEAT})
            except Exception:
                pass

    async def _wait_response(
        self, match: Callable[[dict], bool], timeout: float = 10.0
    ) -> dict[str, Any]:
        response = asyncio.create_task(self._wait_response_legacy(match, timeout))
        closed = asyncio.create_task(self._closed_event.wait())
        try:
            done, _ = await asyncio.wait(
                [response, closed], timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
            if response in done:
                return response.result()
            if closed in done:
                raise ConnectionError(
                    "Threadify connection closed; operation outcome may be unknown"
                )
            raise asyncio.TimeoutError("response timeout")
        finally:
            response.cancel()
            closed.cancel()
            await asyncio.gather(response, closed, return_exceptions=True)

    async def _request(self, message: dict[str, Any], timeout: float = 10.0) -> dict[str, Any]:
        from uuid import uuid4

        from threadify.waiting import ThreadifyError, validate_timeout

        validate_timeout(timeout)
        if not self._connected:
            raise ConnectionError("Threadify connection closed")
        request_id = str(uuid4())
        future = asyncio.get_running_loop().create_future()
        self._requests[request_id] = future
        deadline = asyncio.get_running_loop().time() + timeout

        async def exchange():
            await self._send({**message, "requestId": request_id})
            return await future

        try:
            response = await asyncio.wait_for(exchange(), timeout)
            if asyncio.get_running_loop().time() >= deadline:
                raise asyncio.TimeoutError()
            if response.get("status") != "success":
                raise ThreadifyError(
                    "THREADIFY_REQUEST_FAILED",
                    response.get("message", "Request failed"),
                    request_id=request_id,
                    response=response,
                    is_duplicate=response.get("isDuplicate", False),
                )
            return response
        except (asyncio.TimeoutError, asyncio.CancelledError) as exc:
            if self._connected and (message.get("await") or message.get("waitFor")):
                try:
                    await asyncio.wait_for(
                        self._send({"action": "cancelWait", "targetRequestId": request_id}), 0.2
                    )
                except Exception:
                    pass
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise ThreadifyError(
                "THREADIFY_WAIT_TIMEOUT",
                "Timed out waiting for Engine response",
                request_id=request_id,
            ) from exc
        finally:
            self._requests.pop(request_id, None)
            if not future.done():
                future.cancel()

    async def _wait_response_legacy(
        self, match: Callable[[dict], bool], timeout: float = 10.0
    ) -> dict[str, Any]:
        """Wait for a response matching the predicate."""
        buffered: list[dict[str, Any]] = []
        async with self._response_wait_lock:
            deadline = asyncio.get_running_loop().time() + timeout
            try:
                while True:
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        raise asyncio.TimeoutError("response timeout")

                    msg = await asyncio.wait_for(self._recv_queue.get(), timeout=remaining)
                    if match(msg):
                        return msg
                    buffered.append(msg)
            finally:
                for msg in buffered:
                    self._recv_queue.put_nowait(msg)

    async def _send(self, msg: dict[str, Any]) -> None:
        """Send a JSON message over the WebSocket."""
        if not self._connected:
            raise ConnectionError("WebSocket is not connected")
        await self._ws.send(json.dumps(msg))

    async def thread(self, thread_key: str, options: ThreadOptions | None = None) -> ThreadInstance:
        """Atomically create or resume an application-owned key.

        Options are creation defaults; resuming loads the stored contract version.
        Terminal threads and explicitly conflicting contracts are rejected.
        """
        from threadify.thread import ThreadInstance

        if not isinstance(thread_key, str) or not thread_key.strip():
            raise ValueError("thread_key must be a non-empty string")
        thread_key = thread_key.strip()
        if len(thread_key.encode("utf-8")) > 1024:
            raise ValueError("thread_key exceeds 1024 bytes")
        if options is not None and not isinstance(options, dict):
            raise TypeError("thread options must be a dictionary")
        opts = options or {}
        unknown = opts.keys() - {"label", "contract", "refs", "tags", "service_name", "role"}
        if unknown:
            raise TypeError(f"unknown thread options: {', '.join(sorted(unknown))}")
        for name in ("label", "contract", "service_name", "role"):
            if name in opts and (not isinstance(opts[name], str) or not opts[name].strip()):
                raise TypeError(f"{name} must be a non-empty string when supplied")
        refs = opts.get("refs", {})
        if not isinstance(refs, dict) or any(
            not isinstance(key, str) or not key.strip() or not isinstance(value, str)
            for key, value in refs.items()
        ):
            raise TypeError("refs must be a dictionary of strings")
        tags = opts.get("tags", [])
        if not isinstance(tags, list) or any(
            not isinstance(tag, str) or not tag.strip() for tag in tags
        ):
            raise ValueError("tags must be a list of non-empty strings")
        message = {
            FIELD_ACTION: "thread",
            "threadKey": thread_key,
            FIELD_SERVICE_NAME: opts.get("service_name") or self._service_name,
        }
        for option, field in (
            ("label", "label"),
            ("contract", FIELD_CONTRACT_NAME),
            ("role", FIELD_ROLE),
            ("refs", FIELD_REFS),
            ("tags", FIELD_TAGS),
        ):
            if option in opts:
                message[field] = opts[option]
        response = await self._request(message)
        thread_id = response.get(FIELD_THREAD_ID)
        if not thread_id or response.get("threadKey") != thread_key:
            raise RuntimeError("Engine returned an invalid thread identity")
        thread = self._threads.get(thread_id)
        if thread is None:
            thread = ThreadInstance(self, thread_id)
            self._threads[thread_id] = thread
        thread.thread_key = thread_key
        thread.label = response.get("label", "")
        thread.contract_id = response.get("contractId") or ""
        thread.contract_name = response.get("contractName") or ""
        thread.contract_version = response.get("contractVersion")
        thread.refs = dict(response.get("refs") or {})
        thread.tags = list(response.get("tags") or [])
        return thread

    async def start(
        self,
        label: str = "",
        contract_name: str = "",
        service_name: str = "",
        refs: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        role: str = "",
    ) -> ThreadInstance:
        """Compatibility API for creating a new thread; prefer ``thread(key, options)``."""
        from threadify.thread import ThreadInstance

        if not self._connected:
            raise ConnectionError("Not connected. Call Threadify.connect() first.")

        effective_service = first_non_empty(service_name, self._service_name)

        message_refs = (refs or {}).copy()
        label_value = label

        message_refs[FIELD_SERVICE_NAME] = effective_service
        if label_value:
            message_refs["label"] = label_value

        msg: dict[str, Any] = {
            FIELD_ACTION: ACTION_START_THREAD,
            FIELD_REFS: message_refs,
        }

        effective_role = role
        if contract_name:
            msg[FIELD_CONTRACT_NAME] = contract_name
            if not effective_role:
                if effective_service:
                    effective_role = effective_service.removesuffix("-service")
                else:
                    effective_role = "participant"

        if effective_role:
            msg[FIELD_ROLE] = effective_role

        # Validate and attach tags if provided
        if tags:
            for t in tags:
                if not isinstance(t, str) or not t.strip():
                    raise ValueError("Each tag must be a non-empty string")
            msg[FIELD_TAGS] = list(tags)

        await self._send(msg)

        resp = await self._wait_response(lambda m: m.get(FIELD_ACTION) == ACTION_START_THREAD)

        if resp.get(FIELD_STATUS) != STATUS_SUCCESS:
            raise RuntimeError(resp.get(FIELD_MESSAGE, "failed to start thread"))

        thread_id = resp[FIELD_THREAD_ID]
        thread = ThreadInstance(
            self,
            thread_id,
            resp.get("contractId") or contract_name,
            effective_role,
            resp.get(FIELD_ACCESS_LEVEL, ""),
            dict(resp.get("refs", message_refs) or {}),
        )
        # Trace-only exporter correlation can resume a contracted thread even
        # when this span omitted the contract; retain the Engine's metadata.
        thread.thread_key = resp.get("threadKey") or None
        thread.label = resp.get("label", label_value)
        thread.contract_name = resp.get("contractName") or contract_name
        thread.contract_version = resp.get("contractVersion")
        thread.tags = list(resp.get("tags", tags) or [])
        self._threads[thread_id] = thread
        self._logger.debug(f"Thread started: {thread_id}")
        return thread

    async def join(
        self,
        token_or_thread_id: str | None = None,
        role: str = "",
        *,
        token: str | None = None,
        thread_id: str | None = None,
    ) -> ThreadInstance:
        from threadify.thread import ThreadInstance

        if not self._connected:
            raise ConnectionError("Not connected. Call Threadify.connect() first.")

        msg: dict[str, Any] = {FIELD_ACTION: ACTION_JOIN_THREAD}

        if token is not None:
            if token_or_thread_id is not None or thread_id is not None:
                raise ValueError("token cannot be combined with token_or_thread_id/thread_id")
            require_non_empty("token", token)
            msg[FIELD_THREAD_TOKEN] = token
        elif thread_id is not None:
            require_non_empty("thread_id", thread_id)
            msg[FIELD_THREAD_ID] = thread_id
            if role:
                msg[FIELD_ROLE] = role
        elif token_or_thread_id is not None:
            require_non_empty("token_or_thread_id", token_or_thread_id)
            # If it has 3 parts (JWT) or is a long string, it's likely a token. Otherwise it's a thread ID.
            is_token = len(token_or_thread_id.split(".")) == 3 or len(token_or_thread_id) > 50
            if is_token and not role:
                msg[FIELD_THREAD_TOKEN] = token_or_thread_id
            else:
                msg[FIELD_THREAD_ID] = token_or_thread_id
                if role:
                    msg[FIELD_ROLE] = role
        else:
            raise ValueError("provide token, thread_id+role, or token_or_thread_id")

        await self._send(msg)

        resp = await self._wait_response(lambda m: m.get(FIELD_ACTION) == ACTION_JOIN_THREAD)

        if resp.get(FIELD_STATUS) != STATUS_SUCCESS:
            raise RuntimeError(resp.get(FIELD_MESSAGE, "failed to join thread"))

        thread_id = resp[FIELD_THREAD_ID]
        thread_role = resp.get(FIELD_ROLE, "")
        thread = ThreadInstance(
            self,
            thread_id,
            resp.get("contractId", ""),
            thread_role,
            resp.get(FIELD_ACCESS_LEVEL, ""),
            None,
        )
        self._threads[thread_id] = thread
        self._logger.debug(f"Joined thread: {thread_id}, Role: {thread_role}")
        return thread

    async def close(self) -> None:
        if not self._connected:
            await self._ws.close()
            return

        msg = {FIELD_ACTION: ACTION_CLOSE_CONNECTION}
        try:
            await self._send(msg)
            await self._wait_response(
                lambda m: m.get(FIELD_ACTION) == ACTION_CLOSE_CONNECTION,
                timeout=5.0,
            )
        except Exception:
            pass
        finally:
            self._connected = False
            self._closed_event.set()
            for pending in self._requests.values():
                if not pending.done():
                    pending.set_exception(
                        ConnectionError(
                            "Threadify connection closed; operation outcome may be unknown"
                        )
                    )
            self._listener_task.cancel()
            self._heartbeat_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            await self._ws.close()

    def subscribe(
        self,
        event: str,
        step_name_or_handler: str | NotificationHandler | None = None,
        handler: NotificationHandler | None = None,
    ) -> Connection:
        """Subscribe to notifications for a step or thread-level event.

        Supports two signatures:

        - ``subscribe(event, handler)`` — thread-level subscription.
        - ``subscribe(event, step_name, handler)`` — step-level subscription.
        """
        # Determine signature: 2-param (thread-level) vs 3-param (step-level)
        step_name: str
        actual_handler: NotificationHandler

        if callable(step_name_or_handler):
            # 2-param: subscribe(event, handler)
            step_name = "global"
            actual_handler = step_name_or_handler
        else:
            # 3-param: subscribe(event, step_name, handler)
            step_name = step_name_or_handler or "global"
            actual_handler = handler  # type: ignore[assignment]

        if actual_handler is None:
            raise ValueError("handler cannot be None")

        source, event_type = _parse_event(event)
        event_types = _build_event_types(source, event_type)
        asyncio.ensure_future(self._send_subscription(step_name, event_types))

        key = f"{event}:{step_name}"
        self._notification_handlers.setdefault(key, []).append(actual_handler)
        return self

    def unsubscribe(self, event: str, step_name: str = "") -> Connection:
        """Unsubscribe from notifications.

        Args:
            event: Event pattern to unsubscribe.
            step_name: Step name (default: global / thread-level).
        """
        target_step = step_name or "global"
        key = f"{event}:{target_step}"
        self._notification_handlers.pop(key, None)

        has_handlers = any(k.endswith(f":{target_step}") for k in self._notification_handlers)
        if not has_handlers:
            asyncio.ensure_future(self._send_unsubscription(target_step))
        return self

    async def _send_subscription(self, step_name: str, event_types: list[str]) -> None:
        if not self._connected:
            return

        existing = self._active_subscriptions.get(step_name, [])
        merged = _merge_unique(existing, event_types)
        if set(existing) == set(merged):
            return

        try:
            await self._send(
                {
                    FIELD_ACTION: ACTION_SUBSCRIBE,
                    FIELD_STEP_NAME: step_name,
                    FIELD_EVENT_TYPES: merged,
                }
            )
        except Exception:
            pass

        self._active_subscriptions[step_name] = merged

    async def _send_unsubscription(self, step_name: str) -> None:
        if not self._connected:
            return
        try:
            await self._send(
                {
                    FIELD_ACTION: ACTION_UNSUBSCRIBE,
                    FIELD_STEP_NAME: step_name,
                }
            )
        except Exception:
            pass
        self._active_subscriptions.pop(step_name, None)

    def _handle_notification(self, data: dict, ack_token: str) -> None:
        from threadify.notification import Notification

        if not data:
            return

        notif_id = data.get(FIELD_NOTIFICATION_ID, "")

        if notif_id in self._processed_notifications:
            self._logger.debug(f"Duplicate notification ignored: {notif_id}")
            self._send_ack(notif_id, data.get(FIELD_THREAD_ID, ""), ack_token)
            return

        self._processed_notifications.add(notif_id)

        # Prevent memory leak — remove oldest if too large
        if len(self._processed_notifications) > self._processed_notifications_max_size:
            # Sets are unordered, but we can pop an arbitrary item
            self._processed_notifications.pop()

        notif = Notification(data, self, ack_token)

        event_pattern = self._get_event_pattern(notif)
        self._trigger_handlers(event_pattern, notif)

        thread = self._threads.get(notif.thread_id)
        if thread:
            thread._handle_notification(notif)

    def _get_event_pattern(self, notif: Notification) -> str:
        source = notif.source or "step"
        event_type = STATUS_SUCCESS
        if notif.notification_type:
            parts = notif.notification_type.split(".", 1)
            if len(parts) == 2:
                event_type = parts[1]

        return f"{source}.{event_type}"

    def _trigger_handlers(self, event_pattern: str, notif: Notification) -> None:
        step_name = notif.step_name
        contract_name = notif.contract_name
        source = event_pattern.split(".", 1)[0]

        keys_to_check = []
        if contract_name:
            keys_to_check.append(f"{event_pattern}:{contract_name}@{step_name}")
        keys_to_check.append(f"{event_pattern}:{step_name}")
        keys_to_check.append(f"{source}.*:{step_name}")
        keys_to_check.append(f"*:{step_name}")

        for key in keys_to_check:
            handlers = self._notification_handlers.get(key, [])
            for handler in handlers:
                try:
                    handler(notif)
                except Exception as exc:
                    self._logger.error(f"Notification handler error: {exc}")

    def _send_ack(self, notification_id: str, thread_id: str, ack_token: str) -> None:
        if not ack_token:
            return
        try:
            asyncio.ensure_future(
                self._send(
                    {
                        FIELD_ACTION: ACTION_ACK_NOTIFICATION,
                        FIELD_NOTIFICATION_ID_ACK: notification_id,
                        FIELD_THREAD_ID_ACK: thread_id,
                        FIELD_ACK_TOKEN: ack_token,
                        FIELD_PROCESSED: True,
                    }
                )
            )
        except Exception:
            pass

    def _get_data_retriever(self) -> DataRetriever:
        from threadify.data_retriever import DataRetriever

        if self._data_retriever is None:
            if not self._graphql_url:
                raise RuntimeError("GraphQL URL not configured")
            self._data_retriever = DataRetriever(self._graphql_url, self._api_key)
        return self._data_retriever

    async def get_thread(self, thread_id: str) -> ArchivedThread:
        return await self._get_data_retriever().get_thread(thread_id)

    async def get_thread_by_ref(
        self, ref_key: str | dict[str, str], ref_value: str | None = None
    ) -> ArchivedThread | None:
        threads = await self._get_data_retriever().get_threads_by_ref(
            ref_key if isinstance(ref_key, dict) else {ref_key: ref_value}, limit=1
        )
        return threads[0] if threads else None

    async def get_threads_by_ref(
        self, query: RefQuery | dict[str, str], **filters: Any
    ) -> list[ArchivedThread]:
        return await self._get_data_retriever().get_threads_by_ref(query, **filters)

    async def get_validation_results(
        self, thread_id: str, step_name: str = ""
    ) -> list[dict[str, Any]]:
        return await self._get_data_retriever().get_validation_results(thread_id, step_name)

    async def get_thread_chain(self, root_id: str, max_depth: int = 3) -> list[ArchivedThread]:
        return await self._get_data_retriever().get_thread_chain(root_id, max_depth)

    def create_span_exporter(self, options: dict[str, Any] | None = None) -> Any:
        """Create an OpenTelemetry SpanExporter wired to this connection.

        Args:
            options: Optional configuration dict. Supported keys:
                - ``refs``: list of attribute keys to map to Threadify refs.
                - ``filters``: list of span-name patterns to drop. A trailing ``*``
                  acts as a wildcard prefix match; otherwise an exact match is used.
                  Example: ``["invoke_llm", "adk.before*", "llm.*"]``.

        Returns:
            A :class:`~threadify.otel_exporter.ThreadifySpanExporter` instance.
        """
        from threadify.otel_exporter import ThreadifySpanExporter

        return ThreadifySpanExporter(self, options or {})

    async def reconnect(self) -> None:
        """Resubscribe to all active subscriptions after a reconnection."""
        if not self._connected:
            raise RuntimeError("Not connected")
        for step_name, event_types in self._active_subscriptions.items():
            await self._send_subscription(step_name, event_types)

    def _remove_thread(self, thread_id: str) -> None:
        self._threads.pop(thread_id, None)


def _parse_event(event: str) -> tuple[str, str]:
    parts = event.split(".", 1)
    source = parts[0] if parts[0] else "*"
    event_type = parts[1] if len(parts) > 1 and parts[1] else "*"
    if source not in {"step", "rule", "thread", "*"}:
        raise ValueError(
            f"Unsupported notification source {source!r}; expected 'step', 'rule', 'thread', or '*'"
        )
    return source, event_type


def _build_event_types(source: str, event_type: str) -> list[str]:
    if source == "*" and event_type == "*":
        return ["step.success", "step.failed", "rule.passed", "rule.violated"]
    if source == "step" and event_type == "*":
        return ["step.success", "step.failed"]
    if source == "rule" and event_type == "*":
        return ["rule.passed", "rule.violated"]
    return [f"{source}.{event_type}"]


def _merge_unique(a: list[str], b: list[str]) -> list[str]:
    return list(set(a) | set(b))
