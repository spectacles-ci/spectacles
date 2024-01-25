from typing import List, Tuple

import pytest

from spectacles.client import LookerClient
from spectacles.exceptions import DataTestError, SpectaclesException
from spectacles.lookml import build_project
from spectacles.validators import DataTestValidator
from spectacles.validators.data_test import DataTest


@pytest.fixture
def validator(looker_client: LookerClient) -> DataTestValidator:
    return DataTestValidator(looker_client)


@pytest.fixture(params=["no_errors", "errors"])
async def tests(
    request: pytest.FixtureRequest, validator: DataTestValidator
) -> List[DataTest]:
    if request.param == "no_errors":
        explore_name = "users"
    else:
        explore_name = "users__fail"

    project = await build_project(
        validator.client, name="eye_exam", filters=[f"eye_exam/{explore_name}"]
    )
    tests = await validator.get_tests(project)
    return tests


@pytest.fixture
async def validation_result(
    validator: DataTestValidator, tests: List[DataTest]
) -> Tuple[DataTestError, ...]:
    errors = await validator.validate(tests)
    return tuple(errors)


def test_correct_number_of_tests_generated(tests: List[DataTest]) -> None:
    assert len(tests) == 2


def test_correct_number_of_errors_returned(
    validation_result: Tuple[DataTestError, ...]
) -> None:
    # Errors should only come from the failing Explore
    if validation_result:
        assert len(validation_result) == 3
        print([error.explore for error in validation_result])
        assert all(
            error.explore in ("users__fail", None) for error in validation_result
        )


async def test_no_data_tests_should_raise_error(validator: DataTestValidator) -> None:
    with pytest.raises(SpectaclesException):
        project = await build_project(
            validator.client, name="eye_exam", filters=["-*/*"]
        )
        await validator.get_tests(project)
