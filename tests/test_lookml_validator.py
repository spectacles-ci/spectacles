import pytest
import vcr
from typing import Iterable

from spectacles.validators import LookMLValidator


@pytest.fixture(scope="class")
def validator(looker_client, record_mode) -> Iterable[LookMLValidator]:
    with vcr.use_cassette(
        "tests/cassettes/test_lookml_validator/fixture_validator_init.yaml",
        match_on=["uri", "method", "raw_body"],
        filter_headers=["Authorization"],
        record_mode=record_mode,
    ):
        validator = LookMLValidator(looker_client, project="eye_exam")
        yield validator


def test_lookml_validator_passes_with_no_errors(validator):
    validator.client.update_workspace("production")
    results = validator.validate()

    assert results["status"] == "passed"
    assert len(results["errors"]) == 0


def test_lookml_validator_fails_with_errors(validator):
    validator.client.update_workspace("dev")
    validator.client.checkout_branch("eye_exam", "pytest-fail-lookml")
    results = validator.validate()

    assert results["status"] == "failed"
    assert len(results["errors"]) == 8