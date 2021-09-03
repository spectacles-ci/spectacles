from typing import Iterable, Tuple, Dict
import pytest
import vcr
import jsonschema
from spectacles.validators import DataTestValidator
from spectacles.exceptions import SpectaclesException


@pytest.fixture(scope="class")
def validator(looker_client, record_mode) -> Iterable[DataTestValidator]:
    with vcr.use_cassette(
        "tests/cassettes/test_data_test_validator/fixture_validator_init.yaml",
        match_on=["uri", "method", "raw_body"],
        filter_headers=["Authorization"],
        record_mode=record_mode,
    ):
        validator = DataTestValidator(looker_client, project="eye_exam")
        yield validator


class TestValidatePass:
    """Test the eye_exam Looker project on master for an explore without errors."""

    @pytest.fixture(scope="class")
    def validator_pass(
        self, record_mode, validator
    ) -> Iterable[Tuple[DataTestValidator, Dict]]:
        with vcr.use_cassette(
            "tests/cassettes/test_data_test_validator/fixture_validator_pass.yaml",
            match_on=["uri", "method", "raw_body", "query"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            validator.build_project(selectors=["eye_exam/users"])
            results = validator.validate()
            yield validator, results

    def test_results_should_conform_to_schema(self, schema, validator_pass):
        results = validator_pass[1]
        jsonschema.validate(results, schema)

    def test_results_have_correct_number_of_elements(self, validator_pass):
        results = validator_pass[1]
        assert len(results["errors"]) == 0
        assert len(results["successes"]) == 2


class TestValidateFail:
    """Test the eye_exam Looker project on master for an explore without errors."""

    @pytest.fixture(scope="class")
    def validator_fail(
        self, record_mode, validator
    ) -> Iterable[Tuple[DataTestValidator, Dict]]:
        with vcr.use_cassette(
            "tests/cassettes/test_data_test_validator/fixture_validator_fail.yaml",
            match_on=["uri", "method", "raw_body", "query"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            validator.build_project(selectors=["eye_exam/users__fail"])
            results = validator.validate()
            yield validator, results

    def test_results_should_conform_to_schema(self, schema, validator_fail):
        results = validator_fail[1]
        jsonschema.validate(results, schema)

    def test_results_have_correct_number_of_elements(self, validator_fail):
        results = validator_fail[1]
        assert len(results["errors"]) == 2
        assert len(results["successes"]) == 1


def test_no_data_tests_should_raise_error(validator):
    with pytest.raises(SpectaclesException):
        validator.build_project(exclusions=["*/*"])
        validator.validate()
