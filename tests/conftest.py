from typing import Iterable
import os
import json
from github import Github as GitHub, Repository
import pytest
from spectacles.client import LookerClient
from spectacles.exceptions import SqlError
from spectacles.lookml import Project, Model, Explore, Dimension
from utils import load_resource


def filter_access_token(response):
    if "access_token" in response["body"]["string"].decode():
        body = json.loads(response["body"]["string"])
        del body["access_token"]
        response["body"]["string"] = json.dumps(body)
    return response


@pytest.fixture(scope="session")
def vcr_config():
    return {"filter_headers": ["Authorization"]}


@pytest.mark.vcr(
    filter_post_data_parameters=["client_id", "client_secret"],
    record_mode="all",
    before_record_response=filter_access_token,
    decode_compressed_response=True,
)
@pytest.fixture(scope="session")
def looker_client() -> Iterable[LookerClient]:
    client = LookerClient(
        base_url="https://spectacles.looker.com",
        client_id=os.environ.get("LOOKER_CLIENT_ID", ""),
        client_secret=os.environ.get("LOOKER_CLIENT_SECRET", ""),
    )
    client.update_workspace("production")
    yield client


@pytest.mark.vcr(decode_compressed_response=True)
@pytest.fixture
def remote_repo() -> Repository:
    access_token = os.environ.get("GITHUB_ACCESS_TOKEN")
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
