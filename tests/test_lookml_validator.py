import pytest
from typing import Iterable

from spectacles.validators import LookMLValidator


@pytest.mark.default_cassette("fixture_validator_init.yaml")
@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@pytest.fixture(scope="class")
def validator(looker_client) -> Iterable[LookMLValidator]:
    validator = LookMLValidator(looker_client)
    yield validator


@pytest.mark.vcr
def test_lookml_validator_passes_with_no_errors(validator):
    validator.client.update_workspace("production")
    results = validator.validate(project="eye_exam")

    assert results["status"] == "passed"
    assert len(results["errors"]) == 0


@pytest.mark.vcr
def test_lookml_validator_fails_with_errors(validator):
    validator.client.update_workspace("dev")
    validator.client.checkout_branch("eye_exam", "pytest-fail-lookml")
    results = validator.validate(project="eye_exam")

    assert results["status"] == "failed"
    assert len(results["errors"]) == 9
