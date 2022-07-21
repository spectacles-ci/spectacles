from typing import List, Tuple
import pytest
from spectacles.client import LookerClient
from spectacles.lookml import build_project
from spectacles.validators import ContentValidator
from spectacles.exceptions import SpectaclesException, ContentError


@pytest.fixture
def validator(looker_client: LookerClient) -> ContentValidator:
    return ContentValidator(looker_client, exclude_personal=True)


@pytest.fixture(params=["no_errors", "errors"])
async def validation_result(
    request: pytest.FixtureRequest, validator: ContentValidator
) -> Tuple[ContentError, ...]:
    if request.param == "no_errors":  # type: ignore[attr-defined]
        explore_name = "users"
    else:
        explore_name = "users__fail"

    project = await build_project(
        validator.client, name="eye_exam", filters=[f"eye_exam/{explore_name}"]
    )
    errors = tuple(await validator.validate(project))
    return errors


def test_correct_number_of_errors_returned(validation_result: Tuple[ContentError, ...]):
    # Errors should only come from the failing Explore
    if validation_result:
        assert len(validation_result) == 2
        assert all(
            error.explore in ("users__fail", "users_deleted")
            for error in validation_result
        )
        titles = [error.metadata["title"] for error in validation_result]
        assert "personal" not in titles


async def test_error_from_excluded_folder_should_be_ignored(
    validator: ContentValidator,
):
    project = await build_project(
        validator.client, name="eye_exam", filters=["eye_exam/users__fail"]
    )
    validator.exclude_folders.append("26")
    validation_result: List[ContentError] = await validator.validate(project)
    assert len(validation_result) == 0


async def test_error_from_included_folder_should_be_returned(
    validator: ContentValidator,
):
    project = await build_project(
        validator.client, name="eye_exam", filters=["eye_exam/users__fail"]
    )
    validator.include_folders.append("26")
    validation_result: List[ContentError] = await validator.validate(project)
    assert len(validation_result) == 2


async def test_excluded_folder_should_take_priority_over_included_folder(
    validator: ContentValidator,
):
    project = await build_project(
        validator.client, name="eye_exam", filters=["eye_exam/users__fail"]
    )
    validator.include_folders.append("26")
    validator.exclude_folders.append("26")
    validation_result: List[ContentError] = await validator.validate(project)
    assert len(validation_result) == 0


async def test_error_from_deleted_explore_should_be_present(
    validator: ContentValidator,
):
    filters = ["eye_exam/*"]
    project = await build_project(
        validator.client,
        name="eye_exam",
        filters=filters,
        include_all_explores=True,
    )
    content_errors = await validator.validate(project)
    titles = [error.metadata["title"] for error in content_errors]
    assert "Users [from deleted explore]" in titles


async def test_non_existing_excluded_folder_should_raise_exception(
    looker_client: LookerClient,
):
    validator = ContentValidator(
        looker_client,
        exclude_personal=True,
        folders=["-9999"],
    )
    project = await build_project(
        validator.client, name="eye_exam", filters=["eye_exam/users"]
    )
    with pytest.raises(SpectaclesException):
        await validator.validate(project)


async def test_non_existing_included_folder_should_raise_exception(
    looker_client: LookerClient,
):
    validator = ContentValidator(
        looker_client,
        exclude_personal=True,
        folders=["9999"],
    )
    project = await build_project(
        validator.client, name="eye_exam", filters=["eye_exam/users"]
    )
    with pytest.raises(SpectaclesException):
        await validator.validate(project)
