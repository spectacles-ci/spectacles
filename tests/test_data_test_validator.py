from typing import Iterable, List
import pytest
import vcr
from spectacles.validators import DataTestValidator
from spectacles.validators.data_test import DataTest
from spectacles.exceptions import DataTestError, SpectaclesException
from spectacles.lookml import build_project


@pytest.fixture(scope="class")
def validator(looker_client, record_mode) -> Iterable[DataTestValidator]:
    with vcr.use_cassette(
        "tests/cassettes/test_data_test_validator/fixture_validator_init.yaml",
        match_on=["uri", "method", "raw_body"],
        filter_headers=["Authorization"],
        record_mode=record_mode,
    ):
        validator = DataTestValidator(looker_client)
        yield validator


class TestValidatePass:
    """Test the eye_exam Looker project on master for an explore without errors."""

    @pytest.fixture(scope="class")
    def tests(self, validator, record_mode) -> Iterable[List[DataTest]]:
        with vcr.use_cassette(
            "tests/cassettes/test_data_test_validator/fixture_validator_pass.yaml",
            match_on=["uri", "method", "raw_body", "query"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            project = build_project(
                validator.client, name="eye_exam", filters=["eye_exam/users"]
            )
            tests: List[DataTest] = validator.get_tests(project)
            yield tests

    @pytest.fixture(scope="class")
    def validator_errors(self, validator, tests, record_mode):
        with vcr.use_cassette(
            "tests/cassettes/test_data_test_validator/fixture_validator_pass.yaml",
            match_on=["uri", "method", "raw_body", "query"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            errors: List[DataTestError] = validator.validate(tests)
            yield errors

    def test_results_have_correct_number_of_elements(self, validator_errors, tests):
        assert len(validator_errors) == 0
        assert len(tests) == 2


class TestValidateFail:
    """Test the eye_exam Looker project on master for an explore without errors."""

    @pytest.fixture(scope="class")
    def tests(self, validator, record_mode) -> Iterable[List[DataTest]]:
        with vcr.use_cassette(
            "tests/cassettes/test_data_test_validator/fixture_validator_fail.yaml",
            match_on=["uri", "method", "raw_body", "query"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            project = build_project(
                validator.client, name="eye_exam", filters=["eye_exam/users__fail"]
            )
            tests: List[DataTest] = validator.get_tests(project)
            yield tests

    @pytest.fixture(scope="class")
    def validator_errors(self, validator, tests, record_mode):
        with vcr.use_cassette(
            "tests/cassettes/test_data_test_validator/fixture_validator_fail.yaml",
            match_on=["uri", "method", "raw_body", "query"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            errors: List[DataTestError] = validator.validate(tests)
            yield errors

    def test_results_have_correct_number_of_elements(self, validator_errors, tests):
        assert len(validator_errors) == 2
        assert len(tests) == 2
        assert len(list(test for test in tests if test.passed)) == 1


def test_no_data_tests_should_raise_error(validator):
    with pytest.raises(SpectaclesException):
        project = build_project(validator.client, name="eye_exam", filters=["-*/*"])
        validator.get_tests(project)
