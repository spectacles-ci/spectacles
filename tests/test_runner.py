import string
import jsonschema
import pytest
from unittest.mock import Mock, patch
from spectacles.client import LookerClient
from spectacles.exceptions import ContentError, DataTestError, SqlError
from spectacles.runner import Runner
from utils import build_validation


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@pytest.mark.parametrize("fail_fast", [True, False])
@patch("spectacles.runner.time_hash", side_effect=tuple(string.ascii_lowercase))
def test_validate_sql_should_work(mock_time_hash, looker_client, fail_fast):
    runner = Runner(looker_client, "eye_exam", remote_reset=True)
    result = runner.validate_sql(
        ref="pytest",
        filters=["eye_exam/users", "eye_exam/users__fail"],
        fail_fast=fail_fast,
    )
    assert result["status"] == "failed"
    assert result["tested"][0]["status"] == "passed"
    assert result["tested"][1]["status"] == "failed"
    if fail_fast:
        assert len(result["errors"]) == 1
    else:
        assert len(result["errors"]) > 1


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.time_hash", side_effect=tuple(string.ascii_lowercase))
def test_validate_content_should_work(mock_time_hash, looker_client):
    runner = Runner(looker_client, "eye_exam")
    result = runner.validate_content(filters=["eye_exam/users", "eye_exam/users__fail"])
    assert result["status"] == "failed"
    assert result["tested"][0]["status"] == "passed"
    assert result["tested"][1]["status"] == "failed"
    assert len(result["errors"]) > 0


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.time_hash", side_effect=tuple(string.ascii_lowercase))
def test_validate_data_tests_should_work(mock_time_hash, looker_client):
    runner = Runner(looker_client, "eye_exam")
    result = runner.validate_data_tests(
        filters=["eye_exam/users", "eye_exam/users__fail"]
    )
    assert result["status"] == "failed"
    assert result["tested"][0]["status"] == "passed"
    assert result["tested"][1]["status"] == "failed"
    assert len(result["errors"]) > 0


@patch("spectacles.validators.data_test.DataTestValidator.get_tests")
@patch("spectacles.validators.data_test.DataTestValidator.validate")
@patch("spectacles.runner.build_project")
@patch("spectacles.runner.LookerBranchManager")
def test_validate_data_tests_returns_valid_schema(
    mock_branch_manager,
    mock_build_project,
    mock_validate,
    mock_get_tests,
    project,
    model,
    explore,
    schema,
):
    error_message = "An error ocurred"

    def add_error_to_project(tests):
        project.models[0].explores[0].queried = True
        project.models[0].explores[0].errors = [
            DataTestError("", "", error_message, "", "", "")
        ]

    model.explores = [explore]
    project.models = [model]
    mock_build_project.return_value = project
    mock_validate.side_effect = add_error_to_project
    runner = Runner(client=Mock(spec=LookerClient), project="eye_exam")
    result = runner.validate_data_tests()
    assert result["status"] == "failed"
    assert result["errors"][0]["message"] == error_message
    jsonschema.validate(result, schema)


@patch("spectacles.validators.content.ContentValidator.validate")
@patch("spectacles.runner.build_project")
@patch("spectacles.runner.LookerBranchManager")
def test_validate_content_returns_valid_schema(
    mock_branch_manager,
    mock_build_project,
    mock_validate,
    project,
    model,
    explore,
    schema,
):
    error_message = "An error ocurred"

    def add_error_to_project(tests):
        project.models[0].explores[0].queried = True
        project.models[0].explores[0].errors = [
            ContentError("", "", error_message, "", "", "", "", "")
        ]

    model.explores = [explore]
    project.models = [model]
    mock_build_project.return_value = project
    mock_validate.side_effect = add_error_to_project
    runner = Runner(client=Mock(spec=LookerClient), project="eye_exam")
    result = runner.validate_content()
    assert result["status"] == "failed"
    assert result["errors"][0]["message"] == error_message
    jsonschema.validate(result, schema)


@patch("spectacles.validators.sql.SqlValidator.create_tests")
@patch("spectacles.validators.sql.SqlValidator.run_tests")
@patch("spectacles.runner.build_project")
@patch("spectacles.runner.LookerBranchManager")
def test_validate_sql_returns_valid_schema(
    mock_branch_manager,
    mock_build_project,
    mock_run_tests,
    mock_create_tests,
    project,
    model,
    explore,
    schema,
):
    error_message = "An error ocurred"

    def add_error_to_project(tests, fail_fast, profile):
        project.models[0].explores[0].queried = True
        project.models[0].explores[0].errors = [SqlError("", "", "", "", error_message)]

    model.explores = [explore]
    project.models = [model]
    mock_build_project.return_value = project
    mock_run_tests.side_effect = add_error_to_project
    runner = Runner(client=Mock(spec=LookerClient), project="eye_exam")
    result = runner.validate_sql()
    assert result["status"] == "failed"
    assert result["errors"][0]["message"] == error_message
    jsonschema.validate(result, schema)


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.time_hash", side_effect=tuple(string.ascii_lowercase))
def test_incremental_sql_with_equal_explores_should_not_error(
    mock_time_hash,
    looker_client,
):
    """Case where all explores compile to the same SQL.

    We expect all explores to be skipped, returning no errors.
    """
    runner = Runner(looker_client, "eye_exam", remote_reset=True)
    result = runner.validate_sql(
        incremental=True,
        ref="pytest-incremental-equal",
        filters=["eye_exam/users", "eye_exam/users__fail"],
        fail_fast=False,
    )
    assert result["status"] == "passed"
    assert len(result["errors"]) == 0


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.time_hash", side_effect=tuple(string.ascii_lowercase))
def test_incremental_sql_with_diff_explores_and_valid_sql_should_not_error(
    mock_time_hash, looker_client
):
    """Case where one explore differs in SQL and has valid SQL.

    We expect the differing explore to be tested and return no errors.
    """
    runner = Runner(looker_client, "eye_exam", remote_reset=True)
    result = runner.validate_sql(
        incremental=True,
        ref="pytest-incremental-valid-diff",
        filters=["eye_exam/users", "eye_exam/users__fail"],
        fail_fast=False,
    )
    assert result["status"] == "passed"
    assert result["tested"][0]["explore"] == "users"
    assert result["tested"][0]["status"] == "passed"
    assert result["tested"][1]["explore"] == "users__fail"
    assert result["tested"][1]["status"] == "skipped"
    assert len(result["errors"]) == 0


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.time_hash", side_effect=tuple(string.ascii_lowercase))
def test_incremental_sql_with_diff_explores_and_invalid_sql_should_error(
    mock_time_hash, looker_client
):
    """Case where one explore differs in SQL and has one SQL error.

    We expect the differing explore to be tested and return one error.
    """
    runner = Runner(looker_client, "eye_exam", remote_reset=True)
    result = runner.validate_sql(
        incremental=True,
        ref="pytest-incremental-invalid-diff",
        filters=["eye_exam/users", "eye_exam/users__fail"],
        fail_fast=False,
    )
    assert result["status"] == "failed"
    assert result["tested"][0]["explore"] == "users"
    assert result["tested"][0]["status"] == "failed"
    assert result["tested"][1]["explore"] == "users__fail"
    assert result["tested"][1]["status"] == "skipped"
    assert len(result["errors"]) == 1


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.time_hash", side_effect=tuple(string.ascii_lowercase))
def test_incremental_sql_with_diff_explores_and_invalid_diff_sql_should_error(
    mock_time_hash, looker_client
):
    """Case where one explore differs in SQL and has two SQL errors, one present in
    the target branch, one not present in the target branch.

    We expect the differing explore to be tested and return one error.
    """
    runner = Runner(looker_client, "eye_exam", remote_reset=True)
    result = runner.validate_sql(
        incremental=True,
        ref="pytest-incremental-invalid-equal",
        filters=["eye_exam/users", "eye_exam/users__fail"],
        fail_fast=False,
    )
    assert result["status"] == "failed"
    assert result["tested"][0]["explore"] == "users"
    assert result["tested"][0]["status"] == "skipped"
    assert result["tested"][1]["explore"] == "users__fail"
    assert result["tested"][1]["status"] == "failed"
    assert len(result["errors"]) == 1


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.time_hash", side_effect=tuple(string.ascii_lowercase))
def test_incremental_sql_with_diff_explores_and_invalid_existing_sql_should_error(
    mock_time_hash, looker_client
):
    """Case where the target branch has many errors, one of which is fixed on the base
    branch.

    We expect the differing explore to be tested and return no errors, since the
    remaining errors already exist for the target.
    """
    runner = Runner(looker_client, "eye_exam", remote_reset=True)
    result = runner.validate_sql(
        incremental=True,
        target="pytest-incremental-dirty-prod",
        ref="pytest-incremental-fix-prod",
        filters=["eye_exam/users", "eye_exam/users__fail"],
        fail_fast=False,
    )
    assert result["status"] == "passed"
    assert result["tested"][0]["explore"] == "users"
    assert result["tested"][0]["status"] == "skipped"
    assert result["tested"][1]["explore"] == "users__fail"
    assert result["tested"][1]["status"] == "passed"
    assert len(result["errors"]) == 0


def test_incremental_same_results_should_not_have_errors():
    base = build_validation("content")
    target = build_validation("content")
    diff = Runner._incremental_results(base, target)
    assert diff["status"] == "passed"
    assert diff["errors"] == []
    assert diff["tested"] == [
        dict(model="ecommerce", explore="orders", status="passed"),
        dict(model="ecommerce", explore="sessions", status="passed"),
        dict(model="ecommerce", explore="users", status="passed"),
    ]


def test_incremental_with_fewer_errors_than_target():
    base = build_validation("content")
    target = build_validation("content")
    base["tested"][2]["status"] = "passed"
    base["errors"] = []
    diff = Runner._incremental_results(base, target)
    assert diff["status"] == "passed"
    assert diff["errors"] == []
    assert diff["tested"] == [
        dict(model="ecommerce", explore="orders", status="passed"),
        dict(model="ecommerce", explore="sessions", status="passed"),
        dict(model="ecommerce", explore="users", status="passed"),
    ]


def test_incremental_with_more_errors_than_target():
    base = build_validation("content")
    target = build_validation("content")
    base["tested"][1]["status"] = "failed"
    extra_errors = [
        dict(
            model="ecommerce",
            explore="users",
            test=None,
            message="Another error occurred",
            metadata={},
        ),
        dict(
            model="ecommerce",
            explore="sessions",
            test=None,
            message="An error occurred",
            metadata={},
        ),
    ]
    base["errors"].extend(extra_errors)
    diff = Runner._incremental_results(base, target)
    assert diff["status"] == "failed"
    assert diff["errors"] == extra_errors
    assert diff["tested"] == [
        dict(model="ecommerce", explore="orders", status="passed"),
        dict(model="ecommerce", explore="sessions", status="failed"),
        dict(model="ecommerce", explore="users", status="failed"),
    ]


def test_incremental_with_fewer_tested_explores_than_target():
    base = build_validation("content")
    target = build_validation("content")
    _ = base["tested"].pop(0)
    extra_error = dict(
        model="ecommerce",
        explore="users",
        test=None,
        message="Another error occurred",
        metadata={},
    )
    base["errors"].append(extra_error)
    diff = Runner._incremental_results(base, target)
    assert diff["status"] == "failed"
    assert diff["errors"] == [extra_error]
    assert diff["tested"] == [
        dict(model="ecommerce", explore="sessions", status="passed"),
        dict(model="ecommerce", explore="users", status="failed"),
    ]
