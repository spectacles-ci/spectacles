from typing import List
from unittest.mock import patch
import pytest
from spectacles.runner import LookerBranchManager, BranchState


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
    assert looker_client.get_active_branch_name(project) == starting_branch
    manager.__exit__()
    assert looker_client.get_active_branch_name(project) == starting_branch


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
    assert looker_client.get_active_branch_name(project) == new_branch
    manager.__exit__()
    assert looker_client.get_active_branch_name(project) == starting_branch


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
    ref = "e2d21d"
    looker_client.update_workspace(project, "production")

    manager = LookerBranchManager(
        looker_client, project, name=starting_branch, commit_ref=ref
    )
    assert manager.original_branch == starting_branch

    manager.__enter__()
    assert len(manager.temp_branches) == 1
    temp_branch = manager.temp_branches[0].temp_branch
    branch_info = looker_client.get_active_branch(project)
    assert branch_info["name"] == temp_branch
    assert branch_info["ref"][:6] == ref
    manager.__exit__()
    branch_info = looker_client.get_active_branch(project)
    assert branch_info["name"] == starting_branch
    assert branch_info["ref"][:6] != ref
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
    ref = "e2d21d"
    looker_client.update_workspace(project, "production")

    new_branch = "pytest"
    assert new_branch != starting_branch
    manager = LookerBranchManager(
        looker_client, project, name=new_branch, commit_ref=ref
    )
    assert manager.original_branch == starting_branch

    manager.__enter__()
    assert len(manager.temp_branches) == 1
    temp_branch = manager.temp_branches[0].temp_branch
    branch_info = looker_client.get_active_branch(project)
    assert branch_info["name"] == temp_branch
    assert branch_info["ref"][:6] == ref
    manager.__exit__()
    branch_info = looker_client.get_active_branch(project)
    assert branch_info["name"] == starting_branch
    assert branch_info["ref"][:6] != ref
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
    assert looker_client.get_active_branch_name(project) == starting_branch
    manager.__exit__()
    assert looker_client.get_active_branch_name(project) == starting_branch


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
    assert looker_client.get_active_branch_name(project) == new_branch
    assert looker_client.get_active_branch_name(dependent_project) == temp_branch
    manager.__exit__()
    assert looker_client.get_active_branch_name(project) == starting_branch
    looker_client.update_workspace(project, "dev")
    assert temp_branch not in looker_client.get_all_branches(dependent_project)


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.time_hash", return_value="abc123")
def test_manage_other_branch_with_ref_import_projects(mock_time_hash, looker_client):
    """User is on branch A, checkout branch B and test.

    The manager should create a new temp branch based on branch B, checkout the temp
    branch, test, checkout branch A, and delete the temp branch.

    We are setting import projects to True. The manager should create a temp branch
    in the dependent project, and clean it up at the end.

    """
    # Set up starting branch and workspace
    project = "eye_exam"
    starting_branch = "master"
    ref = "e2d21d"
    dependent_project = "welcome_to_looker"
    looker_client.update_workspace(project, "production")

    new_branch = "pytest"
    assert new_branch != starting_branch
    manager = LookerBranchManager(
        looker_client, project, name=new_branch, import_projects=True, commit_ref=ref
    )
    assert manager.original_branch == starting_branch

    manager.__enter__()
    assert len(manager.temp_branches) == 2
    temp_branches: List[BranchState] = manager.temp_branches.copy()
    for state in temp_branches:
        branch_info = looker_client.get_active_branch(state.project)
        if state.project == project:
            assert branch_info["ref"][:6] == ref
        assert branch_info["name"] == state.temp_branch

    manager.__exit__()
    branch_info = looker_client.get_active_branch(project)
    assert branch_info["name"] == starting_branch
    assert branch_info["ref"][:6] != ref
    looker_client.update_workspace(project, "dev")
    all_branches = set(looker_client.get_all_branches(dependent_project))
    # Confirm that no temp branches still remain
    assert set(state.temp_branch for state in temp_branches).isdisjoint(all_branches)
