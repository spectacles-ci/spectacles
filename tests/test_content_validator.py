from typing import Iterable, List
import pytest
from spectacles.lookml import build_project
from spectacles.validators import ContentValidator
from spectacles.exceptions import ContentError, SpectaclesException


@pytest.mark.default_cassette("fixture_validator_init.yaml")
@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@pytest.fixture(scope="class")
def validator(looker_client) -> Iterable[ContentValidator]:
    validator = ContentValidator(looker_client, exclude_personal=True)
    yield validator


def test_get_content_type_with_bad_keys_should_raise_key_error(validator):
    content = {"lookml_dashboard": "Something goes here."}
    with pytest.raises(KeyError):
        validator._get_content_type(content)


def test_get_tile_type_with_bad_keys_should_raise_key_error(validator):
    content = {"lookml_dashboard": "Something goes here."}
    with pytest.raises(KeyError):
        validator._get_tile_type(content)


class TestValidatePass:
    """Test the eye_exam Looker project on master for an explore without errors."""

    @pytest.mark.default_cassette("fixture_validator_pass.yaml")
    @pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
    @pytest.fixture(scope="class")
    def validator_errors(self, validator) -> Iterable[List[ContentError]]:
        filters = ["eye_exam/users"]
        project = build_project(
            validator.client,
            name="eye_exam",
            filters=filters,
            include_all_explores=True,
        )
        validator.validate(project)
        errors = project.get_results(validator="content", filters=filters)["errors"]
        yield errors

    def test_should_not_return_errors(self, validator_errors):
        assert len(validator_errors) == 0


class TestValidateFail:
    """Test the eye_exam Looker project on master for an explore without errors."""

    @pytest.mark.default_cassette("fixture_validator_fail.yaml")
    @pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
    @pytest.fixture(scope="class")
    def validator_errors(self, validator) -> Iterable[List[ContentError]]:
        filters = ["eye_exam/users__fail"]
        project = build_project(
            validator.client,
            name="eye_exam",
            filters=filters,
            include_all_explores=True,
        )
        validator.validate(project)
        errors = project.get_results(validator="content", filters=filters)["errors"]
        yield errors

    def test_should_return_errors(self, validator_errors):
        assert len(validator_errors) == 1

    def test_personal_folder_content_should_not_be_present(self, validator_errors):
        titles = [error["metadata"]["title"] for error in validator_errors]
        # All failing content in personal spaces has been tagged with "[personal]"
        assert "personal" not in titles


class TestValidateFailExcludeFolder:
    """Test the eye_exam Looker project on master for an explore without errors."""

    @pytest.mark.default_cassette("fixture_validator_fail_excl.yaml")
    @pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
    @pytest.fixture(scope="class")
    def validator_errors(self, validator) -> Iterable[List[ContentError]]:
        filters = ["eye_exam/users__fail"]
        project = build_project(
            validator.client,
            name="eye_exam",
            filters=filters,
            include_all_explores=True,
        )
        validator.excluded_folders.append(26)
        validator.validate(project)
        errors = project.get_results(validator="content", filters=filters)["errors"]
        yield errors

    def test_error_from_excluded_folder_should_be_ignored(self, validator_errors):
        assert len(validator_errors) == 0


class TestValidateFailIncludeFolder:
    """Test the eye_exam Looker project on master for an explore without errors."""

    @pytest.mark.default_cassette("fixture_validator_fail_incl.yaml")
    @pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
    @pytest.fixture(scope="class")
    def validator_errors(self, validator) -> Iterable[List[ContentError]]:
        filters = ["eye_exam/users__fail"]
        project = build_project(
            validator.client,
            name="eye_exam",
            filters=filters,
            include_all_explores=True,
        )
        validator.included_folders.append(26)
        validator.validate(project)
        errors = project.get_results(validator="content", filters=filters)["errors"]
        yield errors

    def test_error_from_included_folder_should_be_returned(self, validator_errors):
        assert len(validator_errors) == 1


class TestValidateFailIncludeExcludeFolder:
    """Test the eye_exam Looker project on master for an explore without errors."""

    @pytest.mark.default_cassette("fixture_validator_fail_incl_excl.yaml")
    @pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
    @pytest.fixture(scope="class")
    def validator_errors(self, validator) -> Iterable[List[ContentError]]:
        filters = ["eye_exam/users__fail"]
        project = build_project(
            validator.client,
            name="eye_exam",
            filters=filters,
            include_all_explores=True,
        )
        validator.included_folders.append(26)
        validator.excluded_folders.append(26)
        validator.validate(project)
        errors = project.get_results(validator="content", filters=filters)["errors"]
        yield errors

    def test_excluded_folder_should_take_priority_over_included_folder(
        self, validator_errors
    ):
        assert len(validator_errors) == 0


def test_non_existing_excluded_folder_should_raise_exception(looker_client):
    with pytest.raises(SpectaclesException):
        ContentValidator(
            looker_client,
            exclude_personal=True,
            folders=["-9999"],
        )


def test_non_existing_included_folder_should_raise_exception(looker_client):
    with pytest.raises(SpectaclesException):
        ContentValidator(
            looker_client,
            exclude_personal=True,
            folders=["9999"],
        )
