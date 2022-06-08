import asyncio
from typing import List, Tuple, Callable, Any
import os
import time
import httpx
import respx
import inspect
import pytest
from unittest.mock import AsyncMock, patch
from spectacles.client import LookerClient, AccessToken
from spectacles.exceptions import SpectaclesException, LookerApiError


def test_expired_access_token_should_be_expired():
    token = AccessToken(
        access_token="abc123",
        token_type="Bearer",
        expires_in=3600,
        expires_at=time.time() - 1,
    )
    assert token.expired


def get_client_method_names() -> List[str]:
    """Extracts method names from LookerClient to test for bad responses"""
    client_members: List[Tuple[str, Callable]] = inspect.getmembers(
        LookerClient, predicate=inspect.isroutine
    )
    client_methods: List[str] = [
        member[0] for member in client_members if not member[0].startswith("__")
    ]
    for skip_method in (
        "authenticate",
        "cancel_query_task",
        "request",
        "get",
        "post",
        "put",
        "patch",
        "delete",
    ):
        client_methods.remove(skip_method)
    return client_methods


@pytest.fixture
def client_kwargs():
    return dict(
        authenticate={"client_id": "", "client_secret": "", "api_version": 3.1},
        get_looker_release_version={},
        get_workspace={},
        update_workspace={"workspace": "dev"},
        checkout_branch={"project": "project_name", "branch": "branch_name"},
        reset_to_remote={"project": "project_name"},
        all_lookml_tests={"project": "project_name"},
        run_lookml_test={"project": "project_name"},
        get_lookml_models={},
        get_lookml_dimensions={"model": "model_name", "explore": "explore_name"},
        create_query={
            "model": "model_name",
            "explore": "explore_name",
            "dimensions": ["dimension_a", "dimension_b"],
        },
        create_query_task={"query_id": 13041},
        get_query_task_multi_results={"query_task_ids": ["ajsdkgj", "askkwk"]},
        create_branch={
            "project": "project_name",
            "branch": "branch_name",
            "ref": "origin/master",
        },
        hard_reset_branch={
            "project": "project_name",
            "branch": "branch_name",
            "ref": "origin/master",
        },
        delete_branch={"project": "project_name", "branch": "branch_name"},
        get_active_branch={"project": "project_name"},
        get_active_branch_name={"project": "project_name"},
        get_manifest={"project": "project_name"},
        get_all_branches={"project": "project_name"},
        content_validation={},
        lookml_validation={"project": "project_name"},
        cached_lookml_validation={"project": "project_name"},
        all_folders={},
        run_query={"query_id": 13041},
    )


@pytest.mark.vcr
def test_get_looker_release_version_should_return_correct_version(
    looker_client: LookerClient,
):
    version = looker_client.get_looker_release_version()
    assert version == "22.8.32"


@pytest.mark.vcr(filter_post_data_parameters=["client_id", "client_secret"])
async def test_bad_authentication_request_should_raise_looker_api_error():
    async with httpx.AsyncClient(trust_env=False) as async_client:
        with pytest.raises(LookerApiError):
            LookerClient(
                async_client=async_client,
                base_url="https://spectacles.looker.com",
                client_id=os.environ.get("LOOKER_CLIENT_ID"),
                client_secret="xxxxxxxxxxxxxx",
            )


@pytest.mark.vcr(filter_post_data_parameters=["client_id", "client_secret"])
async def test_unsupported_api_version_should_raise_error():
    async with httpx.AsyncClient(trust_env=False) as async_client:
        with pytest.raises(SpectaclesException):
            LookerClient(
                async_client=async_client,
                base_url="https://spectacles.looker.com",
                client_id=os.environ.get("LOOKER_CLIENT_ID"),
                client_secret=os.environ.get("LOOKER_CLIENT_SECRET"),
                api_version=3.0,
            )


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
async def test_create_query_with_dimensions_should_return_certain_fields(
    looker_client: LookerClient,
):
    query = await looker_client.create_query(
        model="eye_exam", explore="users", dimensions=["id", "age"]
    )
    assert set(("id", "share_url")) <= set(query.keys())
    assert int(query["limit"]) == 0
    assert query["filter_expression"] == "1=2"
    assert query["model"] == "eye_exam"


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
async def test_create_query_without_dimensions_should_return_certain_fields(
    looker_client: LookerClient,
):
    query = await looker_client.create_query(
        model="eye_exam", explore="users", dimensions=[]
    )
    assert set(("id", "share_url")) <= set(query.keys())
    assert int(query["limit"]) == 0
    assert query["fields"] is None
    assert query["filter_expression"] == "1=2"
    assert query["model"] == "eye_exam"


@patch("spectacles.client.LookerClient.request")
@pytest.mark.parametrize("method_name", get_client_method_names())
async def test_bad_requests_should_raise_looker_api_errors(
    mock_request: AsyncMock,
    method_name: str,
    looker_client: LookerClient,
    client_kwargs: dict[str, dict[str, Any]],
):
    """Tests each method of LookerClient for how it handles a 401 response"""
    response = httpx.Response(
        401, request=httpx.Request("POST", "https://spectacles.looker.com")
    )
    mock_request.return_value = response
    client_method = getattr(looker_client, method_name)
    with pytest.raises(LookerApiError):
        if client_method.__name__ == "get_looker_release_version":
            # This is one method where we don't use LookerClient.request, so have to
            # patch httpx.get directly instead
            with patch("spectacles.client.httpx.get", return_value=response):
                client_method(**client_kwargs[method_name])
        elif asyncio.iscoroutinefunction(client_method):
            await client_method(**client_kwargs[method_name])
        else:
            client_method(**client_kwargs[method_name])


@respx.mock(base_url="https://spectacles.looker.com:19999/api/3.1")
async def test_authenticate_should_set_session_headers(respx_mock: respx.MockRouter):
    respx_mock.post("/login").mock(
        return_value=httpx.Response(
            status_code=200,
            json={
                "access_token": "test_access_token",
                "token_type": "Bearer",
                "expires_in": 3600,
            },
        )
    )
    respx_mock.get("/versions").mock(
        return_value=httpx.Response(
            status_code=200,
            json={"looker_release_version": "0.0.0"},
        )
    )

    async with httpx.AsyncClient(trust_env=False) as async_client:
        client = LookerClient(
            async_client, "https://spectacles.looker.com", "client_id", "client_secret"
        )
        assert client.async_client.headers["Authorization"] == "token test_access_token"
