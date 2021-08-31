from typing import Iterable, Tuple, Dict
import pytest
import jsonschema
import vcr
from spectacles.validators import ContentValidator


@pytest.fixture(scope="class")
def validator(looker_client, record_mode) -> Iterable[ContentValidator]:
    with vcr.use_cassette(
        "tests/cassettes/test_content_validator/fixture_validator_init.yaml",
        match_on=["uri", "method", "raw_body"],
        filter_headers=["Authorization"],
        record_mode=record_mode,
    ):
        validator = ContentValidator(
            looker_client, project="eye_exam", exclude_personal=True
        )
        yield validator


def test_get_content_type_with_bad_keys_should_raise_key_error(validator):
    content = {"lookml_dashboard": "Something goes here."}
    with pytest.raises(KeyError):
        validator._get_content_type(content)


def test_get_tile_type_with_bad_keys_should_raise_key_error(validator):
    content = {"lookml_dashboard": "Something goes here."}
    with pytest.raises(KeyError):
        validator._get_content_type(content)


class TestValidatePass:
    """Test the eye_exam Looker project on master for an explore without errors."""

    @pytest.fixture(scope="class")
    def validator_pass(
        self, record_mode, validator
    ) -> Iterable[Tuple[ContentValidator, Dict]]:
        with vcr.use_cassette(
            "tests/cassettes/test_content_validator/fixture_validator_pass.yaml",
            match_on=["uri", "method", "raw_body"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            validator.build_project(selectors=["eye_exam/users"])
            results = validator.validate()
            yield validator, results

    def test_should_set_errored_and_queried(self, validator_pass):
        validator = validator_pass[0]
        assert validator.project.errored is False
        assert validator.project.queried is True

    def test_results_should_conform_to_schema(self, schema, validator_pass):
        results = validator_pass[1]
        jsonschema.validate(results, schema)


class TestValidateFail:
    """Test the eye_exam Looker project on master for an explore without errors."""

    @pytest.fixture(scope="class")
    def validator_fail(
        self, record_mode, validator
    ) -> Iterable[Tuple[ContentValidator, Dict]]:
        with vcr.use_cassette(
            "tests/cassettes/test_content_validator/fixture_validator_fail.yaml",
            match_on=["uri", "method", "raw_body"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            validator.build_project(selectors=["eye_exam/users__fail"])
            results = validator.validate()
            yield validator, results

    def test_should_set_errored_and_queried(self, validator_fail):
        validator = validator_fail[0]
        assert validator.project.errored is True
        assert validator.project.queried is True

    def test_results_should_conform_to_schema(self, schema, validator_fail):
        results = validator_fail[1]
        jsonschema.validate(results, schema)

    def test_personal_folder_content_should_not_be_present(self, validator_fail):
        results = validator_fail[1]
        titles = [error["metadata"]["title"] for error in results["errors"]]
        # All failing content in personal spaces has been tagged with "[personal]"
        assert "personal" not in titles


class TestValidateFailExcludeFolder:
    """Test the eye_exam Looker project on master for an explore without errors."""

    @pytest.fixture(scope="class")
    def validator_fail_with_exclude(
        self, record_mode, validator
    ) -> Iterable[Tuple[ContentValidator, Dict]]:
        with vcr.use_cassette(
            "tests/cassettes/test_content_validator/fixture_validator_fail.yaml",
            match_on=["uri", "method", "raw_body"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            validator.build_project(selectors=["eye_exam/users__fail"])
            validator.excluded_folders.append(26)
            results = validator.validate()
            yield validator, results

    def test_personal_folder_content_should_not_be_present(
        self, validator_fail_with_exclude
    ):
        results = validator_fail_with_exclude[1]
        assert len(results["errors"]) == 0


class TestValidateFailIncludeFolder:
    """Test the eye_exam Looker project on master for an explore without errors."""

    @pytest.fixture(scope="class")
    def validator_fail_with_include(
        self, record_mode, validator
    ) -> Iterable[Tuple[ContentValidator, Dict]]:
        with vcr.use_cassette(
            "tests/cassettes/test_content_validator/fixture_validator_fail.yaml",
            match_on=["uri", "method", "raw_body"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            validator.build_project(selectors=["eye_exam/users__fail"])
            validator.included_folders.append(26)
            results = validator.validate()
            yield validator, results

    def test_personal_folder_content_should_not_be_present(
        self, validator_fail_with_include
    ):
        results = validator_fail_with_include[1]
        assert len(results["errors"]) == 1


class TestValidateFailIncludeExcludeFolder:
    """Test the eye_exam Looker project on master for an explore without errors."""

    @pytest.fixture(scope="class")
    def validator_fail_with_include_exclude(
        self, record_mode, validator
    ) -> Iterable[Tuple[ContentValidator, Dict]]:
        with vcr.use_cassette(
            "tests/cassettes/test_content_validator/fixture_validator_fail.yaml",
            match_on=["uri", "method", "raw_body"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            validator.build_project(selectors=["eye_exam/users__fail"])
            validator.included_folders.append(26)
            validator.excluded_folders.append(26)
            results = validator.validate()
            yield validator, results

    def test_personal_folder_content_should_not_be_present(
        self, validator_fail_with_include_exclude
    ):
        results = validator_fail_with_include_exclude[1]
        assert len(results["errors"]) == 0
