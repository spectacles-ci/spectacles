from typing import Tuple

import pytest

from spectacles.client import LookerClient
from spectacles.lookml import Explore, build_project
from spectacles.validators.sql import SqlValidator


@pytest.fixture
def validator(looker_client: LookerClient) -> SqlValidator:
    return SqlValidator(looker_client)


@pytest.fixture(
    params=[
        (
            "no_sql_errors",
            "json_bi",
        ),
        (
            "sql_errors",
            "json_bi",
        ),
        (
            "no_sql_errors",
            "json_detail",
        ),
        (
            "sql_errors",
            "json_detail",
        ),
    ]
)
async def explores(
    request: pytest.FixtureRequest, validator: SqlValidator
) -> Tuple[Explore, ...]:
    """Returns Explores from eye_exam/user after SQL validation."""
    errors_state, result_format = request.param
    if errors_state == "no_sql_errors":
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
    await validator.search(explores, fail_fast=False, result_format=result_format)
    return explores


def test_explores_should_be_queried(explores: Tuple[Explore, ...]) -> None:
    assert all(explore.queried for explore in explores)


def test_explores_errored_should_be_set_correctly(
    explores: Tuple[Explore, ...],
) -> None:
    assert len(explores) == 1
    explore = explores[0]
    if explore.name == "users":
        assert not explore.errored
    else:
        assert explore.errored


def test_ignored_dimensions_should_not_be_queried(
    explores: Tuple[Explore, ...],
) -> None:
    for explore in explores:
        assert not any(dim.queried for dim in explore.dimensions if dim.ignore is True)
