import json
from unittest.mock import AsyncMock

import pytest

from threadify.data_retriever import (
    ArchivedStep,
    ArchivedThread,
    DataRetriever,
    GraphQLClient,
)
from threadify.models import RefQuery


class FakeResponse:
    def __init__(self, status_code: int, data: dict):
        self.status_code = status_code
        self._data = data
        self.text = json.dumps(data)

    def json(self):
        return self._data


def _mock_client(response_data: dict, status_code: int = 200) -> GraphQLClient:
    client = GraphQLClient.__new__(GraphQLClient)
    client._url = "https://example.com/graphql"
    client._api_key = "test-key"
    client._client = AsyncMock()
    client._client.post = AsyncMock(return_value=FakeResponse(status_code, response_data))
    return client


class TestGraphQLClient:
    @pytest.mark.asyncio
    async def test_success(self):
        client = _mock_client(
            {
                "data": {"thread": {"id": "t-1", "status": "completed"}},
            }
        )

        data = await client.query("query { thread { id } }")
        assert data["thread"]["id"] == "t-1"

    @pytest.mark.asyncio
    async def test_graphql_errors(self):
        client = _mock_client(
            {
                "data": None,
                "errors": [{"message": "Thread not found"}],
            }
        )

        with pytest.raises(RuntimeError, match="Thread not found"):
            await client.query("query { thread { id } }")

    @pytest.mark.asyncio
    async def test_http_error(self):
        client = _mock_client(
            {"error": "Internal Server Error"},
            status_code=500,
        )

        with pytest.raises(RuntimeError, match="500"):
            await client.query("query { }")


class TestDataRetriever:
    @pytest.mark.asyncio
    async def test_get_thread(self):
        client = _mock_client(
            {
                "data": {
                    "thread": {
                        "id": "t-dr-001",
                        "contractName": "order_flow",
                        "status": "completed",
                        "refs": '{"orderId":"ORD-123"}',
                    },
                },
            }
        )

        dr = DataRetriever.__new__(DataRetriever)
        dr._client = client

        thread = await dr.get_thread("t-dr-001")
        assert thread.id == "t-dr-001"
        assert thread.contract_name == "order_flow"
        assert thread.status == "completed"
        assert thread.refs.get("orderId") == "ORD-123"

    @pytest.mark.asyncio
    async def test_get_thread_not_found(self):
        client = _mock_client(
            {
                "data": {"thread": None},
            }
        )

        dr = DataRetriever.__new__(DataRetriever)
        dr._client = client

        with pytest.raises(RuntimeError, match="not found"):
            await dr.get_thread("missing")

    @pytest.mark.asyncio
    async def test_get_threads_by_ref(self):
        client = _mock_client(
            {
                "data": {
                    "threadsByRef": [
                        {"id": "t-ref-1", "status": "completed"},
                        {"id": "t-ref-2", "status": "in_progress"},
                    ],
                },
            }
        )

        dr = DataRetriever.__new__(DataRetriever)
        dr._client = client

        threads = await dr.get_threads_by_ref(RefQuery(ref_key="orderId", ref_value="ORD-123"))
        assert len(threads) == 2
        assert threads[0].id == "t-ref-1"
        assert threads[1].id == "t-ref-2"

    @pytest.mark.asyncio
    async def test_get_threads_by_ref_empty(self):
        client = _mock_client(
            {
                "data": {"threadsByRef": None},
            }
        )

        dr = DataRetriever.__new__(DataRetriever)
        dr._client = client

        threads = await dr.get_threads_by_ref(RefQuery(ref_key="x", ref_value="y"))
        assert threads == []

    @pytest.mark.asyncio
    async def test_get_thread_chain(self):
        client = _mock_client(
            {
                "data": {
                    "threadChain": [
                        {"id": "root-1", "status": "completed"},
                        {"id": "child-1", "status": "completed"},
                    ],
                },
            }
        )

        dr = DataRetriever.__new__(DataRetriever)
        dr._client = client

        chain = await dr.get_thread_chain("root-1", 3)
        assert len(chain) == 2

    @pytest.mark.asyncio
    async def test_get_thread_chain_requires_root_id(self):
        client = _mock_client({"data": {"threadChain": []}})

        dr = DataRetriever.__new__(DataRetriever)
        dr._client = client

        with pytest.raises(RuntimeError, match="root_id is required"):
            await dr.get_thread_chain("")

    @pytest.mark.asyncio
    async def test_get_thread_chain_defaults_non_positive_depth(self):
        dr = DataRetriever.__new__(DataRetriever)
        dr._client = _mock_client({"data": {"threadChain": []}})
        dr._client.query = AsyncMock(return_value={"threadChain": []})

        await dr.get_thread_chain("root-1", 0)

        call_args = dr._client.query.call_args
        variables = call_args.args[1]
        assert variables["maxDepth"] == 3


class TestArchivedThread:
    @pytest.mark.asyncio
    async def test_steps(self):
        client = _mock_client(
            {
                "data": {
                    "thread": {
                        "steps": [
                            {
                                "stepName": "order_placed",
                                "status": "success",
                                "idempotencyKey": "abc",
                                "history": [
                                    {
                                        "attempt": 1,
                                        "status": "success",
                                        "timestamp": "2026-01-01T00:00:00Z",
                                    },
                                ],
                            },
                        ],
                    },
                },
            }
        )

        thread = ArchivedThread({"id": "t-steps"}, client)
        steps = await thread.steps()

        assert len(steps) == 1
        assert steps[0].step_name == "order_placed"
        assert steps[0].status == "success"
        assert steps[0].last_execution is not None

    @pytest.mark.asyncio
    async def test_steps_requires_thread_id(self):
        thread = ArchivedThread({}, _mock_client({"data": {"thread": {"steps": []}}}))
        with pytest.raises(RuntimeError, match="thread ID is required"):
            await thread.steps()

    @pytest.mark.asyncio
    async def test_validation_results(self):
        client = _mock_client(
            {
                "data": {
                    "thread": {
                        "validationResults": [
                            {"overallStatus": "violated", "criticalCount": 1},
                        ],
                    },
                },
            }
        )

        thread = ArchivedThread({"id": "t-val"}, client)
        results = await thread.validation_results()

        assert len(results) == 1
        assert results[0]["overallStatus"] == "violated"

    @pytest.mark.asyncio
    async def test_get_complete_data(self):
        client = _mock_client(
            {
                "data": {
                    "thread": {
                        "id": "t-complete",
                        "status": "completed",
                        "steps": [{"stepName": "s1"}],
                        "validationResults": [{"overallStatus": "passed"}],
                    },
                },
            }
        )

        thread = ArchivedThread({"id": "t-complete"}, client)
        data = await thread.get_complete_data()

        assert data["id"] == "t-complete"
        assert len(data["steps"]) == 1

    @pytest.mark.asyncio
    async def test_refs_from_json_string(self):
        thread = ArchivedThread(
            {"id": "t-refs", "refs": '{"key":"value"}'}, _mock_client({"data": {}})
        )
        assert thread.refs == {"key": "value"}

    @pytest.mark.asyncio
    async def test_refs_from_dict(self):
        thread = ArchivedThread(
            {"id": "t-refs", "refs": {"key": "value"}}, _mock_client({"data": {}})
        )
        assert thread.refs == {"key": "value"}

    @pytest.mark.asyncio
    async def test_refs_invalid_json(self):
        thread = ArchivedThread({"id": "t-refs", "refs": "not-json"}, _mock_client({"data": {}}))
        assert thread.refs == {}


class TestArchivedStep:
    @pytest.mark.asyncio
    async def test_history(self):
        client = _mock_client(
            {
                "data": {
                    "stepHistory": [
                        {"attempt": 1, "status": "failed"},
                        {"attempt": 2, "status": "success"},
                    ],
                },
            }
        )

        step = ArchivedStep(
            {"threadId": "t-1", "stepName": "payment", "idempotencyKey": "k"},
            client,
        )
        history = await step.history()

        assert len(history) == 2
        assert history[0]["status"] == "failed"
        assert history[1]["status"] == "success"

    @pytest.mark.asyncio
    async def test_sub_steps(self):
        client = _mock_client(
            {
                "data": {
                    "thread": {
                        "steps": [
                            {
                                "subSteps": [
                                    {"name": "sub1", "status": "success"},
                                ],
                            },
                        ],
                    },
                },
            }
        )

        step = ArchivedStep(
            {"threadId": "t-1", "stepName": "main"},
            client,
        )
        subs = await step.sub_steps()

        assert len(subs) == 1
        assert subs[0]["name"] == "sub1"

    @pytest.mark.asyncio
    async def test_last_execution_populated(self):
        step = ArchivedStep(
            {
                "stepName": "s",
                "history": [{"attempt": 1, "status": "success"}],
            },
            _mock_client({"data": {}}),
        )
        assert step.last_execution is not None
        assert step.last_execution["status"] == "success"

    @pytest.mark.asyncio
    async def test_last_execution_empty(self):
        step = ArchivedStep(
            {"stepName": "s"},
            _mock_client({"data": {}}),
        )
        assert step.last_execution is None
