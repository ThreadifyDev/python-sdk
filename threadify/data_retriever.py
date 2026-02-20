from __future__ import annotations

import json
from typing import Any

import httpx

from threadify.models import (
    CompleteDataOptions,
    HistoryQueryOptions,
    RefQuery,
)

THREAD_FIELDS = """
    id
    contractId
    contractName
    contractVersion
    ownerId
    companyId
    status
    lastHash
    startedAt
    completedAt
    error
    refs
"""

STEP_FIELDS = """
    threadId
    stepName
    idempotencyKey
    status
    retryCount
    firstSeenAt
    lastUpdatedAt
    latestStepID
    previousStep
    verified
    verificationError
"""

STEP_HISTORY_FIELDS = """
    attempt
    timestamp
    status
    context
    duration
    error
"""

SUB_STEP_FIELDS = """
    id
    threadId
    stepId
    name
    status
    payload
    recordedAt
"""

VALIDATION_RESULT_FIELDS = """
    validationId
    threadId
    stepId
    stepName
    idempotencyKey
    timestamp
    validations {
        type
        message
        field
        expected
        actual
        rule
    }
    overallStatus
    hasCriticalViolation
    criticalCount
    warningCount
    infoCount
"""


class GraphQLClient:
    """Performs authenticated GraphQL requests."""

    def __init__(self, url: str, api_key: str):
        self._url = url
        self._api_key = api_key
        self._client = httpx.AsyncClient()

    async def query(
        self, gql_query: str, variables: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Execute a GraphQL query and return the data portion."""
        body = {"query": gql_query, "variables": variables or {}}

        resp = await self._client.post(
            self._url,
            json=body,
            headers={
                "Content-Type": "application/json",
                "X-API-Key": self._api_key,
            },
        )

        if resp.status_code != 200:
            raise RuntimeError(f"GraphQL request failed: {resp.status_code} {resp.text}")

        result = resp.json()

        if result.get("errors"):
            raise RuntimeError(
                f"GraphQL errors: {result['errors'][0].get('message', 'Unknown error')}"
            )

        return result.get("data", {})

    async def close(self) -> None:
        await self._client.aclose()


class DataRetriever:
    """Read access to archived thread data via GraphQL."""

    def __init__(self, graphql_url: str, api_key: str):
        self._client = GraphQLClient(graphql_url, api_key)

    async def get_thread(self, thread_id: str) -> ArchivedThread:
        """Retrieve an archived thread by ID."""
        query = f"""
            query GetThread($id: ID!) {{
                thread(id: $id) {{
                    {THREAD_FIELDS}
                }}
            }}
        """
        data = await self._client.query(query, {"id": thread_id})
        thread_data = data.get("thread")
        if not thread_data:
            raise RuntimeError(f"Thread not found: {thread_id}")
        return ArchivedThread(thread_data, self._client)

    async def get_threads_by_ref(self, q: RefQuery) -> list[ArchivedThread]:
        """Retrieve threads by reference key-value pair."""
        query = f"""
            query GetThreadsByRef(
                $refKey: String!
                $refValue: String!
                $status: String
                $startedAfter: String
                $startedBefore: String
                $limit: Int
                $offset: Int
            ) {{
                threadsByRef(
                    refKey: $refKey
                    refValue: $refValue
                    status: $status
                    startedAfter: $startedAfter
                    startedBefore: $startedBefore
                    limit: $limit
                    offset: $offset
                ) {{
                    {THREAD_FIELDS}
                }}
            }}
        """
        variables: dict[str, Any] = {
            "refKey": q.ref_key,
            "refValue": q.ref_value,
            "limit": q.limit or 50,
            "offset": q.offset or 0,
        }
        if q.status:
            variables["status"] = q.status
        if q.started_after:
            variables["startedAfter"] = q.started_after
        if q.started_before:
            variables["startedBefore"] = q.started_before
        data = await self._client.query(query, variables)
        threads_list = data.get("threadsByRef") or []
        return [ArchivedThread(t, self._client) for t in threads_list if isinstance(t, dict)]

    async def get_thread_chain(self, root_id: str, max_depth: int = 3) -> list[ArchivedThread]:
        """Retrieve a thread chain from the root."""
        if not root_id:
            raise RuntimeError("root_id is required")
        if max_depth <= 0:
            max_depth = 3

        query = f"""
            query GetThreadChain($rootId: ID!, $maxDepth: Int) {{
                threadChain(rootId: $rootId, maxDepth: $maxDepth) {{
                    {THREAD_FIELDS}
                }}
            }}
        """
        data = await self._client.query(
            query,
            {
                "rootId": root_id,
                "maxDepth": max_depth,
            },
        )
        chain_list = data.get("threadChain") or []
        return [ArchivedThread(t, self._client) for t in chain_list if isinstance(t, dict)]


class ArchivedThread:
    """Historical thread with read-only access."""

    def __init__(self, data: dict[str, Any], client: GraphQLClient):
        self.id: str = data.get("id", "")
        self.contract_id: str = data.get("contractId", "")
        self.contract_name: str = data.get("contractName", "")
        self.contract_version: str = data.get("contractVersion", "")
        self.owner_id: str = data.get("ownerId", "")
        self.company_id: str = data.get("companyId", "")
        self.status: str = data.get("status", "")
        self.last_hash: str = data.get("lastHash", "")
        self.started_at: str = data.get("startedAt", "")
        self.completed_at: str = data.get("completedAt", "")
        self.error: str = data.get("error", "")

        # Parse refs from JSON string.
        refs_raw = data.get("refs", "")
        if isinstance(refs_raw, str) and refs_raw:
            try:
                self.refs: dict[str, Any] = json.loads(refs_raw)
            except (json.JSONDecodeError, TypeError):
                self.refs = {}
        elif isinstance(refs_raw, dict):
            self.refs = refs_raw
        else:
            self.refs = {}

        self._client = client

    async def steps(
        self,
        step_name: str = "",
        idempotency_key: str = "",
        status: str = "",
    ) -> list[ArchivedStep]:
        """Retrieve steps for this thread, optionally filtered."""
        if not self.id:
            raise RuntimeError("thread ID is required")

        query = f"""
            query GetThreadSteps(
                $threadId: ID!
                $stepName: String
                $idempotencyKey: String
                $status: String
            ) {{
                thread(id: $threadId) {{
                    steps(stepName: $stepName, idempotencyKey: $idempotencyKey, status: $status) {{
                        {STEP_FIELDS}
                        history(limit: 1) {{
                            {STEP_HISTORY_FIELDS}
                        }}
                    }}
                }}
            }}
        """
        variables: dict[str, Any] = {"threadId": self.id}
        if step_name:
            variables["stepName"] = step_name
        if idempotency_key:
            variables["idempotencyKey"] = idempotency_key
        if status:
            variables["status"] = status

        data = await self._client.query(query, variables)
        thread_data = data.get("thread") or {}
        steps_list = thread_data.get("steps") or []
        return [ArchivedStep(s, self._client) for s in steps_list if isinstance(s, dict)]

    async def validation_results(self, limit: int = 10) -> list[dict[str, Any]]:
        """Retrieve validation results for this thread."""
        if limit <= 0:
            limit = 10

        query = f"""
            query GetThreadValidations($threadId: ID!, $options: ValidationQueryOptions) {{
                thread(id: $threadId) {{
                    validationResults(options: $options) {{
                        {VALIDATION_RESULT_FIELDS}
                    }}
                }}
            }}
        """
        data = await self._client.query(
            query,
            {
                "threadId": self.id,
                "options": {"limit": limit},
            },
        )
        thread_data = data.get("thread") or {}
        return thread_data.get("validationResults") or []

    async def get_complete_data(self, options: CompleteDataOptions | None = None) -> dict[str, Any]:
        """Retrieve complete thread data (steps + history + validations) in one query."""
        opts = options or CompleteDataOptions()
        step_history_limit = opts.step_history_limit if opts.step_history_limit > 0 else 50
        validation_limit = opts.validation_limit if opts.validation_limit > 0 else 10

        query = f"""
            query GetCompleteThread(
                $id: ID!
                $stepName: String
                $idempotencyKey: String
                $status: String
                $stepHistoryLimit: Int
                $validationLimit: Int
            ) {{
                thread(id: $id) {{
                    {THREAD_FIELDS}
                    steps(stepName: $stepName, idempotencyKey: $idempotencyKey, status: $status) {{
                        {STEP_FIELDS}
                        history(limit: $stepHistoryLimit) {{
                            {STEP_HISTORY_FIELDS}
                        }}
                    }}
                    validationResults(options: {{limit: $validationLimit}}) {{
                        {VALIDATION_RESULT_FIELDS}
                    }}
                }}
            }}
        """
        variables: dict[str, Any] = {
            "id": self.id,
            "stepHistoryLimit": step_history_limit,
            "validationLimit": validation_limit,
        }
        if opts.step_name:
            variables["stepName"] = opts.step_name
        if opts.idempotency_key:
            variables["idempotencyKey"] = opts.idempotency_key
        if opts.status:
            variables["status"] = opts.status

        data = await self._client.query(query, variables)
        thread_data = data.get("thread")
        if not thread_data:
            raise RuntimeError(f"Thread not found: {self.id}")
        return thread_data


class ArchivedStep:
    """Historical step with read-only access."""

    def __init__(self, data: dict[str, Any], client: GraphQLClient):
        self.thread_id: str = data.get("threadId", "")
        self.step_name: str = data.get("stepName", "")
        self.idempotency_key: str = data.get("idempotencyKey", "")
        self.status: str = data.get("status", "")
        self.retry_count: int = int(data.get("retryCount", 0))
        self.first_seen_at: str = data.get("firstSeenAt", "")
        self.last_updated_at: str = data.get("lastUpdatedAt", "")
        self.latest_step_id: str = data.get("latestStepID", "")
        self.previous_step: str = data.get("previousStep", "")
        self.verified: bool = data.get("verified", False)
        self.verification_error: str = data.get("verificationError", "")

        # Last execution from history.
        history = data.get("history") or []
        self.last_execution: dict[str, Any] | None = history[0] if history else None

        self._client = client

    async def history(self, options: HistoryQueryOptions | None = None) -> list[dict[str, Any]]:
        """Retrieve execution history for this step."""
        opts = options or HistoryQueryOptions()
        limit = opts.limit if opts.limit > 0 else 100

        query = f"""
            query GetStepHistory(
                $threadId: String!
                $stepName: String!
                $idempotencyKey: String
                $limit: Int
                $offset: Int
                $startAt: String
                $endAt: String
                $activityType: String
                $actor: String
            ) {{
                stepHistory(
                    threadId: $threadId
                    stepName: $stepName
                    idempotencyKey: $idempotencyKey
                    limit: $limit
                    offset: $offset
                    startAt: $startAt
                    endAt: $endAt
                    activityType: $activityType
                    actor: $actor
                ) {{
                    {STEP_HISTORY_FIELDS}
                }}
            }}
        """
        variables: dict[str, Any] = {
            "threadId": self.thread_id,
            "stepName": self.step_name,
            "limit": limit,
        }
        if self.idempotency_key:
            variables["idempotencyKey"] = self.idempotency_key
        if opts.offset:
            variables["offset"] = opts.offset
        if opts.start_at:
            variables["startAt"] = opts.start_at
        if opts.end_at:
            variables["endAt"] = opts.end_at
        if opts.activity_type:
            variables["activityType"] = opts.activity_type
        if opts.actor:
            variables["actor"] = opts.actor

        data = await self._client.query(query, variables)
        return data.get("stepHistory") or []

    async def sub_steps(self) -> list[dict[str, Any]]:
        """Retrieve sub-steps for this step."""
        query = f"""
            query GetStepSubSteps(
                $threadId: String!
                $stepName: String!
                $idempotencyKey: String
            ) {{
                thread(id: $threadId) {{
                    steps(stepName: $stepName, idempotencyKey: $idempotencyKey) {{
                        subSteps {{
                            {SUB_STEP_FIELDS}
                        }}
                    }}
                }}
            }}
        """
        variables: dict[str, Any] = {
            "threadId": self.thread_id,
            "stepName": self.step_name,
        }
        if self.idempotency_key:
            variables["idempotencyKey"] = self.idempotency_key

        data = await self._client.query(query, variables)
        thread_data = data.get("thread") or {}
        steps_list = thread_data.get("steps") or []
        if not steps_list:
            return []
        first_step = steps_list[0] if isinstance(steps_list[0], dict) else {}
        return first_step.get("subSteps") or []
