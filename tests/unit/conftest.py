from typing import AsyncIterable, Iterable

import httpx
import pytest
import respx

from spectacles.client import LookerClient


@pytest.fixture
def mocked_api() -> Iterable[respx.MockRouter]:
    with respx.mock(
        base_url="https://spectacles.looker.com:19999/api/4.0", assert_all_called=False
    ) as respx_mock:
        respx_mock.post("/login", name="login").mock(
            return_value=httpx.Response(
                status_code=200,
                json={
                    "access_token": "<ACCESS TOKEN>",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "refresh_token": None,
                },
            )
        )
        respx_mock.get("/versions", name="get_looker_api_version").mock(
            return_value=httpx.Response(
                status_code=200,
                json={"looker_release_version": "0.0.0"},
            )
        )
        respx_mock.post("/versions", name="queries").mock(
            return_value=httpx.Response(
                status_code=200,
                json={"looker_release_version": "0.0.0"},
            )
        )
        respx_mock.patch("/session", name="update_workspace").respond(200)
        yield respx_mock


@pytest.fixture
async def looker_client(mocked_api: respx.MockRouter) -> AsyncIterable[LookerClient]:
    async with httpx.AsyncClient(trust_env=False) as async_client:
        client = LookerClient(
            async_client=async_client,
            base_url="https://spectacles.looker.com",
            client_id="",
            client_secret="",
        )
        await client.update_workspace("production")
        yield client
