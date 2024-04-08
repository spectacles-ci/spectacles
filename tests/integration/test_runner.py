import re
import string
from unittest.mock import MagicMock, patch

import pytest

from spectacles.client import LookerClient
from spectacles.runner import Runner


@pytest.fixture(autouse=True)
async def cleanup_tmp_branches(looker_client: LookerClient) -> None:
    for project in ("eye_exam", "looker-demo"):
        await looker_client.update_workspace("dev")
        branches_json = await looker_client.get_all_branches(project)
        branches = [branch["name"] for branch in branches_json]

        to_delete = []
        dev_branch = None
        for branch in branches:
            if not dev_branch and branch.startswith("dev"):
                dev_branch = branch
            elif re.match("tmp_spectacles_[a-z]$", branch):
                to_delete.append(branch)

        if to_delete and dev_branch:
            # In case we're currently on a branch we want to delete
            try:
                await looker_client.checkout_branch(project, dev_branch)
            except Exception:
                pass
            for branch in to_delete:
                await looker_client.delete_branch(project, branch)


@pytest.mark.parametrize("fail_fast", [True, False])
@patch("spectacles.runner.time_hash", side_effect=tuple(string.ascii_lowercase))
async def test_validate_sql_should_work(
    mock_time_hash: MagicMock, looker_client: LookerClient, fail_fast: bool
) -> None:
    runner = Runner(looker_client, "eye_exam", remote_reset=True)
    result = await runner.validate_sql(
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


@patch("spectacles.runner.time_hash", side_effect=tuple(string.ascii_lowercase))
async def test_validate_content_should_work(
    mock_time_hash: MagicMock, looker_client: LookerClient
) -> None:
    runner = Runner(looker_client, "eye_exam")
    result = await runner.validate_content(
        filters=["eye_exam/users", "eye_exam/users__fail"]
    )
    assert result["status"] == "failed"
    assert result["tested"][0]["status"] == "passed"
    assert result["tested"][1]["status"] == "failed"
    assert len(result["errors"]) > 0


@patch("spectacles.runner.time_hash", side_effect=tuple(string.ascii_lowercase))
async def test_validate_data_tests_should_work(
    mock_time_hash: MagicMock, looker_client: LookerClient
) -> None:
    runner = Runner(looker_client, "eye_exam")
    result = await runner.validate_data_tests(
        filters=["eye_exam/users", "eye_exam/users__fail"]
    )
    assert result["status"] == "failed"
    assert result["tested"][0]["status"] == "passed"
    assert result["tested"][1]["status"] == "failed"
    assert len(result["errors"]) > 0


@patch("spectacles.runner.time_hash", side_effect=tuple(string.ascii_lowercase))
async def test_incremental_sql_with_equal_explores_should_not_error(
    mock_time_hash: MagicMock, looker_client: LookerClient
) -> None:
    """Case where all explores compile to the same SQL.

    We expect all explores to be skipped, returning no errors.
    """
    runner = Runner(looker_client, "eye_exam", remote_reset=True)
    result = await runner.validate_sql(
        incremental=True,
        ref="pytest-incremental-equal",
        filters=["eye_exam/users", "eye_exam/users__fail"],
        fail_fast=False,
    )
    assert result["status"] == "passed"
    assert len(result["errors"]) == 0


@patch("spectacles.runner.time_hash", side_effect=tuple(string.ascii_lowercase))
async def test_incremental_sql_with_diff_explores_and_valid_sql_should_not_error(
    mock_time_hash: MagicMock, looker_client: LookerClient
) -> None:
    """Case where one explore differs in SQL and has valid SQL.

    We expect the differing explore to be tested and return no errors.
    """
    runner = Runner(looker_client, "eye_exam", remote_reset=True)
    result = await runner.validate_sql(
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


@patch("spectacles.runner.time_hash", side_effect=tuple(string.ascii_lowercase))
async def test_incremental_sql_with_diff_explores_and_invalid_sql_should_error(
    mock_time_hash: MagicMock, looker_client: LookerClient
) -> None:
    """Case where one explore differs in SQL and has one SQL error.

    We expect the differing explore to be tested and return one error.
    """
    runner = Runner(looker_client, "eye_exam", remote_reset=True)
    result = await runner.validate_sql(
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


@patch("spectacles.runner.time_hash", side_effect=tuple(string.ascii_lowercase))
async def test_incremental_sql_with_diff_explores_and_invalid_diff_sql_should_error(
    mock_time_hash: MagicMock, looker_client: LookerClient
) -> None:
    """Case where one explore differs in SQL and has two SQL errors, one present in
    the target branch, one not present in the target branch.

    We expect the differing explore to be tested and return one error.
    """
    runner = Runner(looker_client, "eye_exam", remote_reset=True)
    result = await runner.validate_sql(
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


@patch("spectacles.runner.time_hash", side_effect=tuple(string.ascii_lowercase))
async def test_incremental_sql_with_diff_explores_and_invalid_existing_sql_should_error(
    mock_time_hash: MagicMock, looker_client: LookerClient
) -> None:
    """Case where the target branch has many errors, one of which is fixed on the base
    branch.

    We expect the differing explore to be tested and return no errors, since the
    remaining errors already exist for the target.
    """
    runner = Runner(looker_client, "eye_exam", remote_reset=True)
    result = await runner.validate_sql(
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


async def test_validate_sql_with_query_profiler_should_work(
    looker_client: LookerClient, caplog: pytest.LogCaptureFixture
) -> None:
    runner = Runner(looker_client, "eye_exam")
    await runner.validate_sql(fail_fast=True, profile=True, runtime_threshold=0)
    assert "Query profiler results" in caplog.text
