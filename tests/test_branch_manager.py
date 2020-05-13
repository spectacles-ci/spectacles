from unittest.mock import patch
import pytest
from spectacles.runner import LookerBranchManager


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
def test_manage_current_branch(looker_client):
    """User is on branch A, checkout branch A and test.

    The manager should not perform any branch checkouts, just test.

    """
    # Set up starting branch and workspace
    project = "eye_exam"
    starting_branch = "master"
    looker_client.update_workspace(project, "production")

    manager = LookerBranchManager(looker_client, project, name=starting_branch)
    assert manager.original_branch == starting_branch

    manager.__enter__()
    assert looker_client.get_active_branch(project) == starting_branch
    manager.__exit__()
    assert looker_client.get_active_branch(project) == starting_branch


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
def test_manage_other_branch(looker_client):
    """User is on branch A, checkout branch B and test.

    The manager should checkout branch B, test, then checkout branch A.

    """
    # Set up starting branch and workspace
    project = "eye_exam"
    starting_branch = "master"
    looker_client.update_workspace(project, "production")

    new_branch = "pytest"
    assert new_branch != starting_branch
    manager = LookerBranchManager(looker_client, project, name=new_branch)
    assert manager.original_branch == starting_branch

    manager.__enter__()
    assert looker_client.get_active_branch(project) == new_branch
    manager.__exit__()
    assert looker_client.get_active_branch(project) == starting_branch


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.time_hash", return_value="abc123")
def test_manage_current_branch_with_ref(mock_time_hash, looker_client):
    """User is on branch A, checkout branch A with a commit ref and test.

    The manager should create a new temp branch based on branch A, checkout the temp
    branch, test, checkout branch A, and delete the temp branch.

    """
    # Set up starting branch and workspace
    project = "eye_exam"
    starting_branch = "master"
    looker_client.update_workspace(project, "production")

    manager = LookerBranchManager(
        looker_client, project, name=starting_branch, commit_ref="e2d21d"
    )
    assert manager.original_branch == starting_branch

    manager.__enter__()
    assert len(manager.temp_branches) == 1
    temp_branch = manager.temp_branches[0].temp_branch
    assert looker_client.get_active_branch(project) == temp_branch
    manager.__exit__()
    assert looker_client.get_active_branch(project) == starting_branch
    looker_client.update_workspace(project, "dev")
    assert temp_branch not in looker_client.get_all_branches(project)


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.time_hash", return_value="abc123")
def test_manage_other_branch_with_ref(mock_time_hash, looker_client):
    """User is on branch A, checkout branch B with a commit ref and test.

    The manager should create a new temp branch based on branch B, checkout the temp
    branch, test, checkout branch A, and delete the temp branch.

    """
    # Set up starting branch and workspace
    project = "eye_exam"
    starting_branch = "master"
    looker_client.update_workspace(project, "production")

    new_branch = "pytest"
    assert new_branch != starting_branch
    manager = LookerBranchManager(
        looker_client, project, name=new_branch, commit_ref="e2d21d"
    )
    assert manager.original_branch == starting_branch

    manager.__enter__()
    assert len(manager.temp_branches) == 1
    temp_branch = manager.temp_branches[0].temp_branch
    assert looker_client.get_active_branch(project) == temp_branch
    manager.__exit__()
    assert looker_client.get_active_branch(project) == starting_branch
    looker_client.update_workspace(project, "dev")
    assert temp_branch not in looker_client.get_all_branches(project)


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.time_hash", return_value="abc123")
def test_manage_current_branch_with_import_projects(mock_time_hash, looker_client):
    """User is on branch A, checkout branch A and test.

    Though we are pasing with import projects set to True, it should not
    temp branches because we are staying in production mode.

    The manager should not perform any branch checkouts, just test.

    """
    # Set up starting branch and workspace
    project = "eye_exam"
    starting_branch = "master"
    looker_client.update_workspace(project, "production")

    manager = LookerBranchManager(
        looker_client, project, name=starting_branch, import_projects=True
    )
    assert manager.original_branch == starting_branch

    manager.__enter__()
    assert len(manager.temp_branches) == 0
    assert looker_client.get_active_branch(project) == starting_branch
    manager.__exit__()
    assert looker_client.get_active_branch(project) == starting_branch


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.time_hash", return_value="abc123")
def test_manage_other_branch_with_import_projects(mock_time_hash, looker_client):
    """User is on branch A, checkout branch B and test.

    The manager should create a new temp branch based on branch B, checkout the temp
    branch, test, checkout branch A, and delete the temp branch.

    We are setting import projects to True. The manager should create a temp branch
    in the dependent project, and clean it up at the end.

    """
    # Set up starting branch and workspace
    project = "eye_exam"
    starting_branch = "master"
    dependent_project = "welcome_to_looker"
    looker_client.update_workspace(project, "production")

    new_branch = "pytest"
    assert new_branch != starting_branch
    manager = LookerBranchManager(
        looker_client, project, name=new_branch, import_projects=True
    )
    assert manager.original_branch == starting_branch

    manager.__enter__()
    assert len(manager.temp_branches) == 1
    temp_branch = manager.temp_branches[0].temp_branch
    assert looker_client.get_active_branch(project) == new_branch
    assert looker_client.get_active_branch(dependent_project) == temp_branch
    manager.__exit__()
    assert looker_client.get_active_branch(project) == starting_branch
    looker_client.update_workspace(project, "dev")
    assert temp_branch not in looker_client.get_all_branches(dependent_project)
