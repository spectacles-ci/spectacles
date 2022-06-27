import pytest
from spectacles.client import LookerClient
from spectacles.validators import LookMLValidator


@pytest.fixture
def validator(looker_client: LookerClient) -> LookMLValidator:
    return LookMLValidator(looker_client)


async def test_lookml_validator_passes_with_no_errors(validator: LookMLValidator):
    await validator.client.update_workspace("production")
    results = await validator.validate(project="eye_exam")

    assert results["status"] == "passed"
    assert len(results["errors"]) == 0


async def test_lookml_validator_fails_with_errors(validator: LookMLValidator):
    await validator.client.update_workspace("dev")
    await validator.client.checkout_branch("eye_exam", "pytest-fail-lookml")
    results = await validator.validate(project="eye_exam")

    assert results["status"] == "failed"
    assert len(results["errors"]) == 7
