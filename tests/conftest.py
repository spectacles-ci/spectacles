from typing import AsyncIterable, Iterable
import os
import json
from github import Github as GitHub
from github.Repository import Repository
import pytest
import httpx
import respx
from respx.fixtures import session_event_loop as event_loop  # noqa: F401
from spectacles.client import LookerClient
from spectacles.exceptions import SqlError
from spectacles.lookml import Project, Model, Explore, Dimension
from utils import load_resource
import vcr
from pathlib import Path


def filter_access_token(response):
    if "content" in response and "access_token" in response["content"]:
        body = json.loads(response["content"])
        body["access_token"] = "<Access token filtered from cassette>"
        response["content"] = json.dumps(body)
    elif "body" in response and "access_token" in response["body"]["string"].decode():
        body = json.loads(response["body"]["string"].decode())
        body["access_token"] = "<Access token filtered from cassette>"
        response["body"]["string"] = json.dumps(body).encode()
    return response


@pytest.fixture(scope="session")
def vcr_config():
    return {"filter_headers": ["Authorization"]}


@pytest.fixture(scope="session")
async def looker_client(event_loop) -> AsyncIterable[LookerClient]:  # noqa: F811
    with vcr.use_cassette(
        path=str(
            Path(__file__).parent
            / "vcr"
            / "cassettes"
            / "fixtures"
            / "looker_client.yaml"
        ),
        filter_post_data_parameters=["client_id", "client_secret"],
        filter_headers=["Authorization"],
        before_record_response=filter_access_token,
    ):
        async with httpx.AsyncClient(trust_env=False) as async_client:
            client = LookerClient(
                async_client=async_client,
                base_url="https://spectacles.looker.com",
                client_id=os.environ.get("LOOKER_CLIENT_ID", ""),
                client_secret=os.environ.get("LOOKER_CLIENT_SECRET", ""),
            )
            await client.update_workspace("production")
            yield client


@pytest.fixture
def mocked_api() -> Iterable[respx.MockRouter]:
    with respx.mock(
        base_url="https://spectacles.looker.com:19999/api/3.1", assert_all_called=False
    ) as respx_mock:
        respx_mock.post("/login", name="login").mock(
            return_value=httpx.Response(
                status_code=200,
                json={
                    "access_token": "<ACCESS TOKEN>",
                    "token_type": "Bearer",
                    "expires_in": 3600,
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
        yield respx_mock


@pytest.fixture(scope="module")
def remote_repo() -> Iterable[Repository]:
    access_token = os.environ.get("GITHUB_ACCESS_TOKEN")
    with vcr.use_cassette(
        path=str(
            Path(__file__).parent
            / "vcr"
            / "cassettes"
            / "fixtures"
            / "remote_repo.yaml"
        ),
        filter_headers=["Authorization"],
        before_record_response=filter_access_token,
        decode_compressed_response=True,
    ):
        client = GitHub(access_token)
        repo = client.get_repo("spectacles-ci/eye-exam")
        yield repo


@pytest.fixture
def dimension():
    return Dimension(
        name="age",
        model_name="eye_exam",
        explore_name="users",
        type="number",
        tags=[],
        sql='${TABLE}."AGE"',
        url="/projects/eye_exam/files/views%2Fusers.view.lkml?line=6",
        is_hidden=False,
    )


@pytest.fixture
def explore():
    return Explore(name="users", model_name="eye_exam")


@pytest.fixture
def model():
    return Model(name="eye_exam", project_name="eye_exam", explores=[])


@pytest.fixture
def project():
    return Project(name="eye_exam", models=[])


@pytest.fixture
def sql_error():
    return SqlError(
        dimension="users.age",
        explore="users",
        model="eye_exam",
        sql="SELECT age FROM users WHERE 1=2 LIMIT 1",
        message="An error occurred.",
        explore_url="https://spectacles.looker.com/x/qCJsodAZ2Y22QZLbmD0Gvy",
    )


@pytest.fixture
def schema():
    return load_resource("validation_schema.json")
