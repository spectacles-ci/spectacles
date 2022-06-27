import pytest
from spectacles.lookml import Explore, build_project
from spectacles.client import LookerClient
from spectacles.validators.sql import SqlValidator


@pytest.fixture
def validator(looker_client: LookerClient) -> SqlValidator:
    return SqlValidator(looker_client)


@pytest.fixture(params=["no_sql_errors", "sql_errors"])
async def explores(
    request: pytest.FixtureRequest, validator: SqlValidator
) -> tuple[Explore, ...]:
    """Returns Explores from eye_exam/user after SQL validation."""
    if request.param == "no_sql_errors":  # type: ignore[attr-defined]
        explore_name = "users"
    else:
        explore_name = "users__fail"

    project = await build_project(
        validator.client,
        name="eye_exam",
        filters=[f"eye_exam/{explore_name}"],
        include_dimensions=True,
    )
    explores = tuple(project.iter_explores())
    await validator.search(explores, fail_fast=False)
    return explores


def test_explores_should_be_queried(explores: tuple[Explore, ...]):
    assert all(explore.queried for explore in explores)


def test_explores_errored_should_be_set_correctly(explores: tuple[Explore, ...]):
    assert len(explores) == 1
    explore = explores[0]
    if explore.name == "users":
        assert not explore.errored
    else:
        assert explore.errored


def test_ignored_dimensions_should_not_be_queried(explores: tuple[Explore, ...]):
    for explore in explores:
        assert not any(dim.queried for dim in explore.dimensions if dim.ignore is True)
