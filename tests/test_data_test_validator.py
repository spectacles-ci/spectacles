from typing import Iterable, List
import pytest
from spectacles.validators import DataTestValidator
from spectacles.validators.data_test import DataTest
from spectacles.exceptions import DataTestError, SpectaclesException
from spectacles.lookml import build_project


@pytest.mark.default_cassette("fixture_validator_init.yaml")
@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@pytest.fixture(scope="class")
def validator(looker_client) -> Iterable[DataTestValidator]:
    validator = DataTestValidator(looker_client)
    yield validator


class TestValidatePass:
    """Test the eye_exam Looker project on master for an explore without errors."""

    @pytest.mark.default_cassette("fixture_validator_pass.yaml")
    @pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
    @pytest.fixture(scope="class")
    def tests(self, validator) -> Iterable[List[DataTest]]:
        project = build_project(
            validator.client, name="eye_exam", filters=["eye_exam/users"]
        )
        tests: List[DataTest] = validator.get_tests(project)
        yield tests

    @pytest.mark.default_cassette("fixture_validator_pass.yaml")
    @pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
    @pytest.fixture(scope="class")
    def validator_errors(self, validator, tests):
        errors: List[DataTestError] = validator.validate(tests)
        yield errors

    def test_results_have_correct_number_of_elements(self, validator_errors, tests):
        assert len(validator_errors) == 0
        assert len(tests) == 2


class TestValidateFail:
    """Test the eye_exam Looker project on master for an explore without errors."""

    @pytest.mark.default_cassette("fixture_validator_fail.yaml")
    @pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
    @pytest.fixture(scope="class")
    def tests(self, validator) -> Iterable[List[DataTest]]:
        project = build_project(
            validator.client, name="eye_exam", filters=["eye_exam/users__fail"]
        )
        tests: List[DataTest] = validator.get_tests(project)
        yield tests

    @pytest.mark.default_cassette("fixture_validator_fail.yaml")
    @pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
    @pytest.fixture(scope="class")
    def validator_errors(self, validator, tests):
        errors: List[DataTestError] = validator.validate(tests)
        yield errors

    def test_results_have_correct_number_of_elements(self, validator_errors, tests):
        assert len(validator_errors) == 3
        assert len(tests) == 2
        assert len(list(test for test in tests if test.passed)) == 0


@pytest.mark.vcr
def test_no_data_tests_should_raise_error(validator):
    with pytest.raises(SpectaclesException):
        project = build_project(validator.client, name="eye_exam", filters=["-*/*"])
        validator.get_tests(project)
