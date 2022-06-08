import os
import pytest
import httpx
from spectacles.client import LookerClient
from spectacles.exceptions import LookerApiError, SpectaclesException


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
