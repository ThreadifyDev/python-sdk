import httpx
import pytest

from threadify.management import EntityProfileManager, ManagementAPIError, profile_slug


def test_profile_slug_matches_server_rules():
    assert profile_slug(" Customer Profile ") == "customer_profile"
    assert profile_slug("CUST---Health") == "cust_health"


@pytest.mark.asyncio
async def test_apply_sends_complete_declaration_and_dry_run():
    seen = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["request"] = request
        return httpx.Response(200, json={"status": "created", "dry_run": True})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    manager = EntityProfileManager(
        "service-key", web_api_url="http://localhost:3001/api/", http_client=client
    )
    declaration = {
        "name": "Customer Profile",
        "description": "Delivery health",
        "type": ["customer_id"],
        "metrics": [{"name": "deliveries", "template_id": "total_deliveries"}],
    }

    result = await manager.apply(declaration, dry_run=True)

    request = seen["request"]
    assert str(request.url) == (
        "http://localhost:3001/api/entity-profile-types/customer_profile?dry_run=true"
    )
    assert request.headers["Authorization"] == "Bearer service-key"
    assert result["status"] == "created"
    await client.aclose()


@pytest.mark.asyncio
async def test_apply_surfaces_api_error_body():
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": "metric names must be unique"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    manager = EntityProfileManager("key", http_client=client)

    with pytest.raises(ManagementAPIError) as exc:
        await manager.apply({"name": "Customer", "type": ["customer_id"], "metrics": []})

    assert exc.value.status_code == 400
    assert "metric names must be unique" in str(exc.value)
    await client.aclose()


@pytest.mark.asyncio
async def test_rename_is_an_explicit_operation():
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/api/entity-profile-types/customer/rename"
        return httpx.Response(200, json={"data": {"name": "Account", "slug": "account"}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    manager = EntityProfileManager("key", web_api_url="http://localhost/api", http_client=client)

    result = await manager.rename("Customer", "Account")

    assert result["data"]["slug"] == "account"
    await client.aclose()
