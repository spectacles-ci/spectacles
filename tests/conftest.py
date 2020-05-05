from typing import Iterable
import os
import vcr
import pytest
from spectacles.client import LookerClient
from spectacles.exceptions import SqlError
from spectacles.lookml import Project, Model, Explore, Dimension
from tests.utils import load_resource


@pytest.fixture(scope="session")
def vcr_config():
    return {"filter_headers": ["Authorization"]}


@pytest.fixture(scope="session")
def looker_client(record_mode) -> Iterable[LookerClient]:
    with vcr.use_cassette(
        "tests/cassettes/init_client.yaml",
        filter_post_data_parameters=["client_id", "client_secret"],
        filter_headers=["Authorization"],
        record_mode=record_mode,
    ):
        client = LookerClient(
            base_url="https://spectacles.looker.com",
            client_id=os.environ.get("LOOKER_CLIENT_ID", ""),
            client_secret=os.environ.get("LOOKER_CLIENT_SECRET", ""),
        )
        client.update_session(project="eye_exam", branch="master", remote_reset=False)
        yield client


@pytest.fixture
def dimension():
    return Dimension(
        name="age",
        model_name="eye_exam",
        explore_name="users",
        type="number",
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
