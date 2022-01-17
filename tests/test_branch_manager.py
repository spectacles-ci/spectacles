from spectacles.client import LookerClient
from unittest.mock import patch
import pytest
from spectacles.runner import LookerBranchManager


LOOKER_PROJECT = "eye_exam"
TMP_REMOTE_BRANCH = "pytest-tmp"


@pytest.fixture
def test_remote_branch(remote_repo):
    """Creates a test branch off master, waits for test execution, then cleans up."""
    branch = remote_repo.get_branch("master")
    remote_repo.create_git_ref(
        ref=f"refs/heads/{TMP_REMOTE_BRANCH}", sha=branch.commit.sha
    )
    yield
    remote_repo.get_git_ref(f"heads/{TMP_REMOTE_BRANCH}").delete()


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.LookerBranchManager.get_project_imports", return_value=[])
def test_should_return_to_initial_state_prod(
    mock_get_imports, looker_client: LookerClient
):
    # Set up starting branch and workspace
    looker_client.update_workspace("production")

    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)
    assert manager.init_state.workspace == "production"
    with manager(ref="pytest"):
        assert looker_client.get_workspace() == "dev"
    assert looker_client.get_workspace() == "production"


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.LookerBranchManager.get_project_imports", return_value=[])
def test_should_return_to_initial_state_dev(
    mock_get_imports, looker_client: LookerClient
):
    # Set up starting branch and workspace
    looker_client.update_workspace("dev")

    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)
    assert manager.init_state.workspace == "dev"
    with manager():
        assert looker_client.get_workspace() == "production"
    assert looker_client.get_workspace() == "dev"


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.LookerBranchManager.get_project_imports", return_value=[])
def test_manage_current_branch(mock_get_imports, looker_client: LookerClient):
    """User is on branch A, checkout branch A and test.

    The manager should not perform any branch checkouts, just test.

    """
    # Set up starting branch and workspace
    looker_client.update_workspace("dev")
    branch = "pytest"
    looker_client.checkout_branch(LOOKER_PROJECT, branch)

    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)
    assert manager.init_state.branch == branch
    manager(ref=branch).__enter__()
    assert looker_client.get_active_branch_name(LOOKER_PROJECT) == branch
    manager.__exit__()
    assert looker_client.get_active_branch_name(LOOKER_PROJECT) == branch


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.LookerBranchManager.get_project_imports", return_value=[])
def test_manage_other_branch(mock_get_imports, looker_client: LookerClient):
    """User is on branch A, checkout branch B and test.

    The manager should checkout branch B, test, then checkout branch A.

    """
    # Set up starting branch and workspace
    looker_client.update_workspace("dev")
    starting_branch = "pytest"
    looker_client.checkout_branch(LOOKER_PROJECT, starting_branch)

    new_branch = "pytest-additional"
    assert new_branch != starting_branch
    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)
    assert manager.init_state.branch == starting_branch

    manager(ref=new_branch).__enter__()
    assert looker_client.get_active_branch_name(LOOKER_PROJECT) == new_branch
    manager.__exit__()
    assert looker_client.get_active_branch_name(LOOKER_PROJECT) == starting_branch


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.LookerBranchManager.get_project_imports", return_value=[])
@patch("spectacles.runner.time_hash", return_value="abc123")
def test_manage_current_branch_with_ref(
    mock_time_hash, mock_get_imports, looker_client: LookerClient
):
    """User is on branch A, checkout branch A with a commit ref and test.

    The manager should create a new temp branch based on branch A, checkout the temp
    branch, test, checkout branch A, and delete the temp branch.

    """
    # Set up starting branch and workspace
    looker_client.update_workspace("dev")
    starting_branch = "pytest"
    looker_client.checkout_branch(LOOKER_PROJECT, starting_branch)
    commit = "e2d21d"

    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)
    assert manager.init_state.branch == starting_branch

    manager(ref=commit).__enter__()
    assert manager.is_temp_branch
    temp_branch = manager.branch
    branch_info = looker_client.get_active_branch(LOOKER_PROJECT)
    assert branch_info["name"] == temp_branch
    assert branch_info["ref"][:6] == commit
    manager.__exit__()
    branch_info = looker_client.get_active_branch(LOOKER_PROJECT)
    assert branch_info["name"] == starting_branch
    assert branch_info["ref"][:6] != commit
    assert temp_branch not in looker_client.get_all_branches(LOOKER_PROJECT)


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.time_hash", return_value="abc123")
def test_manage_current_branch_with_import_projects(
    mock_time_hash, looker_client: LookerClient
):
    """User is on branch A, checkout branch A and test.

    We should not import any temp branches because we are staying in the production
    workspace.

    The manager should not perform any branch checkouts, just test.

    """
    # Set up starting branch and workspace
    starting_branch = "master"
    looker_client.update_workspace("production")

    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)
    assert manager.init_state.branch == starting_branch

    manager().__enter__()
    assert not manager.is_temp_branch
    assert looker_client.get_active_branch_name(LOOKER_PROJECT) == starting_branch
    manager.__exit__()
    assert looker_client.get_active_branch_name(LOOKER_PROJECT) == starting_branch


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.time_hash", return_value="abc123")
def test_manage_other_branch_with_import_projects(
    mock_time_hash, looker_client: LookerClient
):
    """User is on branch A, checkout branch B and test.

    The manager should checkout branch B, test, then checkout branch A.

    We are setting import projects to True. The manager should create a temp branch
    in the dependent project, and clean it up at the end.

    """
    # Set up starting branch and workspace
    looker_client.update_workspace("dev")
    starting_branch = "pytest"
    looker_client.checkout_branch(LOOKER_PROJECT, starting_branch)
    dependent_project = "welcome_to_looker"

    new_branch = "pytest-additional"
    assert new_branch != starting_branch
    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)
    assert manager.init_state.branch == starting_branch

    manager(ref=new_branch).__enter__()
    assert not manager.is_temp_branch
    dependent_project_manager = manager.import_managers[0]
    assert dependent_project_manager.is_temp_branch
    temp_branch = dependent_project_manager.branch
    assert looker_client.get_active_branch_name(LOOKER_PROJECT) == new_branch
    assert looker_client.get_active_branch_name(dependent_project) == temp_branch
    manager.__exit__()
    assert looker_client.get_active_branch_name(LOOKER_PROJECT) == starting_branch
    assert (
        looker_client.get_active_branch_name(dependent_project)
        == dependent_project_manager.init_state.branch
    )
    assert temp_branch not in looker_client.get_all_branches(dependent_project)


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.LookerBranchManager.get_project_imports", return_value=[])
def test_manage_prod_with_advanced_deploy(
    mock_get_imports, looker_client: LookerClient
):
    # Set up starting branch and workspace
    project = "spectacles-advanced-deploy"
    looker_client.update_workspace("production")
    commit = looker_client.get_active_branch(project)["ref"]

    manager = LookerBranchManager(looker_client, project)
    assert manager.init_state.workspace == "production"
    assert manager.init_state.commit == commit
    with manager():
        assert looker_client.get_workspace() == "production"


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.time_hash", return_value="abc123")
def test_manage_with_ref_import_projects(mock_time_hash, looker_client: LookerClient):
    """User is on branch A, checkout branch B and test.

    The manager should create a new temp branch based on branch B, checkout the temp
    branch, test, checkout branch A, and delete the temp branch.

    We are setting import projects to True. The manager should create a temp branch
    in the dependent project, and clean it up at the end.

    """
    # Set up starting branch and workspace
    looker_client.update_workspace("production")
    dependent_project = "welcome_to_looker"
    commit = "e2d21d"

    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)
    assert manager.init_state.workspace == "production"
    branch_info = looker_client.get_active_branch(LOOKER_PROJECT)
    assert branch_info["ref"][:6] != commit

    with manager(ref=commit):
        assert manager.is_temp_branch
        assert manager.commit and manager.commit[:6] == commit
        branch_info = looker_client.get_active_branch(manager.project)
        assert branch_info["ref"][:6] == commit
        for import_manager in manager.import_managers:
            branch = looker_client.get_active_branch_name(import_manager.project)
            assert import_manager.branch == branch

    branch_info = looker_client.get_active_branch(LOOKER_PROJECT)
    assert branch_info["ref"][:6] != commit

    looker_client.update_workspace("dev")
    all_branches = set(looker_client.get_all_branches(dependent_project))
    # Confirm that no temp branches still remain
    temp_branches = set(
        import_manager.branch for import_manager in manager.import_managers
    )
    assert temp_branches.isdisjoint(all_branches)


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
@patch("spectacles.runner.LookerBranchManager.get_project_imports", return_value=[])
@patch("spectacles.runner.time_hash", return_value="abc123")
def test_manage_with_ref_not_present_in_local_repo(
    mock_time_hash,
    mock_get_imports,
    remote_repo,
    test_remote_branch,
    looker_client: LookerClient,
):
    # Create a commit on an external branch directly on the GitHub remote
    content = remote_repo.get_contents("README.md", ref="master")
    result = remote_repo.update_file(
        content.path,
        message="Updating file",
        content=".",
        sha=content.sha,
        branch=TMP_REMOTE_BRANCH,
    )
    commit = result["commit"].sha

    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)
    with manager(ref=commit):
        assert manager.is_temp_branch
        assert manager.commit == commit
        branch_info = looker_client.get_active_branch(LOOKER_PROJECT)
        assert branch_info["ref"] == branch_info["remote_ref"]
        assert branch_info["ref"] == commit
