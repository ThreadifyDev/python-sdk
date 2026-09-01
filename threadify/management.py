from __future__ import annotations

import re
from typing import Any, Mapping

import httpx


class ManagementAPIError(RuntimeError):
    """Raised when the Threadify management API rejects a declaration."""

    def __init__(self, status_code: int, message: str, body: Any = None):
        super().__init__(f"Threadify management API returned HTTP {status_code}: {message}")
        self.status_code = status_code
        self.body = body


def profile_slug(name: str) -> str:
    """Normalize a profile name using the Web API's canonical slug rules."""
    return re.sub(r"^_+|_+$", "", re.sub(r"[^a-z0-9]+", "_", name.lower()))


class EntityProfileManager:
    """Thin client for declaratively managing Threadify entity profile types."""

    def __init__(
        self,
        api_key: str,
        *,
        web_api_url: str = "https://web.threadify.dev/api",
        http_client: httpx.AsyncClient | None = None,
    ):
        if not api_key.strip():
            raise ValueError("api_key is required")
        self._api_key = api_key
        self._base_url = web_api_url.rstrip("/")
        self._client = http_client or httpx.AsyncClient()
        self._owns_client = http_client is None

    @staticmethod
    def _raise_for_error(response: httpx.Response) -> None:
        if not response.is_error:
            return
        try:
            body = response.json()
        except ValueError:
            body = response.text
        message = (
            body.get("error", response.reason_phrase)
            if isinstance(body, dict)
            else body
        )
        raise ManagementAPIError(response.status_code, str(message), body)

    async def apply(
        self,
        declaration: Mapping[str, Any],
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Apply or plan a complete profile declaration.

        Reconciliation is performed by Threadify. The declaration must contain
        ``name`` and the complete desired ``type`` and ``metrics`` collections.
        """
        name = str(declaration.get("name", "")).strip()
        slug = profile_slug(name)
        if not slug:
            raise ValueError("declaration.name is required")

        response = await self._client.put(
            f"{self._base_url}/entity-profile-types/{slug}",
            params={"dry_run": "true"} if dry_run else None,
            json=dict(declaration),
            headers={"Authorization": f"Bearer {self._api_key}"},
        )
        self._raise_for_error(response)
        return response.json()

    async def archive(self, name: str) -> None:
        """Explicitly archive a profile type by its name-derived slug."""
        slug = profile_slug(name)
        if not slug:
            raise ValueError("name is required")
        response = await self._client.delete(
            f"{self._base_url}/entity-profile-types/{slug}",
            headers={"Authorization": f"Bearer {self._api_key}"},
        )
        self._raise_for_error(response)

    async def rename(self, current_name: str, new_name: str) -> dict[str, Any]:
        """Explicitly rename a profile and therefore change its slug identity."""
        current_slug = profile_slug(current_name)
        if not current_slug or not profile_slug(new_name):
            raise ValueError("current_name and new_name are required")
        response = await self._client.post(
            f"{self._base_url}/entity-profile-types/{current_slug}/rename",
            json={"name": new_name},
            headers={"Authorization": f"Bearer {self._api_key}"},
        )
        self._raise_for_error(response)
        return response.json()

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> EntityProfileManager:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
