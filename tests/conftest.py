import pytest

from spectacles.exceptions import SqlError
from spectacles.lookml import Dimension, Explore, Model, Project
from spectacles.models import JsonDict
from tests.utils import load_resource


@pytest.fixture
def dimension() -> Dimension:
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
def explore() -> Explore:
    return Explore(name="users", model_name="eye_exam")


@pytest.fixture
def model() -> Model:
    return Model(name="eye_exam", project_name="eye_exam", explores=[])


@pytest.fixture
def project() -> Project:
    return Project(name="eye_exam", models=[])


@pytest.fixture
def sql_error() -> SqlError:
    return SqlError(
        dimension="users.age",
        explore="users",
        model="eye_exam",
        sql="SELECT age FROM users WHERE 1=2 LIMIT 1",
        message="An error occurred.",
        explore_url="https://spectacles.looker.com/x/qCJsodAZ2Y22QZLbmD0Gvy",
    )


@pytest.fixture
def schema() -> JsonDict:
    return load_resource("validation_schema.json")  # type: ignore
