import os
from typing import Iterable
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from github import Github as GitHub
from github.Repository import Repository

from spectacles.client import LookerClient
from spectacles.runner import LookerBranchManager

LOOKER_PROJECT = "eye_exam"
TMP_REMOTE_BRANCH = "pytest-tmp"


@pytest.fixture(scope="module")
def remote_repo() -> Iterable[Repository]:
    access_token = os.environ["GITHUB_ACCESS_TOKEN"]
    client = GitHub(access_token)
    repo = client.get_repo("spectacles-ci/eye-exam")
    yield repo


@pytest.fixture(scope="module", autouse=True)
def test_remote_branch(remote_repo: Repository) -> Iterable[None]:
    """Creates a test branch off master, waits for test execution, then cleans up."""
    branch = remote_repo.get_branch("master")
    remote_repo.create_git_ref(
        ref=f"refs/heads/{TMP_REMOTE_BRANCH}", sha=branch.commit.sha
    )
    yield
    remote_repo.get_git_ref(f"heads/{TMP_REMOTE_BRANCH}").delete()


@patch("spectacles.runner.LookerBranchManager.get_project_imports", return_value=[])
async def test_should_return_to_initial_state_prod(
    mock_get_imports: AsyncMock, looker_client: LookerClient
) -> None:
    # Set up starting branch and workspace
    await looker_client.update_workspace("production")

    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)
    async with manager(ref="pytest"):
        assert manager.init_state.workspace == "production"
        workspace = await looker_client.get_workspace()
    assert workspace == "dev"
    workspace = await looker_client.get_workspace()
    assert workspace == "production"


@patch("spectacles.runner.LookerBranchManager.get_project_imports", return_value=[])
async def test_should_return_to_initial_state_dev(
    mock_get_imports: AsyncMock, looker_client: LookerClient
) -> None:
    # Set up starting branch and workspace
    await looker_client.update_workspace("dev")

    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)
    async with manager():
        assert manager.init_state.workspace == "dev"
        workspace = await looker_client.get_workspace()
    assert workspace == "production"
    workspace = await looker_client.get_workspace()
    assert workspace == "dev"


@patch("spectacles.runner.LookerBranchManager.get_project_imports", return_value=[])
async def test_manage_current_branch(
    mock_get_imports: AsyncMock, looker_client: LookerClient
) -> None:
    """User is on branch A, checkout branch A and test.

    The manager should not perform any branch checkouts, just test.

    """
    # Set up starting branch and workspace
    await looker_client.update_workspace("dev")
    branch = "pytest"
    await looker_client.checkout_branch(LOOKER_PROJECT, branch)

    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)
    await manager(ref=branch).__aenter__()
    assert manager.init_state.branch == branch
    active_branch = await looker_client.get_active_branch_name(LOOKER_PROJECT)
    assert active_branch == branch
    await manager.__aexit__()
    active_branch = await looker_client.get_active_branch_name(LOOKER_PROJECT)
    assert active_branch == branch


@patch("spectacles.runner.LookerBranchManager.get_project_imports", return_value=[])
async def test_manage_other_branch(
    mock_get_imports: AsyncMock, looker_client: LookerClient
) -> None:
    """User is on branch A, checkout branch B and test.

    The manager should checkout branch B, test, then checkout branch A.

    """
    # Set up starting branch and workspace
    await looker_client.update_workspace("dev")
    starting_branch = "pytest"
    await looker_client.checkout_branch(LOOKER_PROJECT, starting_branch)

    new_branch = "pytest-additional"
    assert new_branch != starting_branch
    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)

    await manager(ref=new_branch).__aenter__()
    assert manager.init_state.branch == starting_branch
    active_branch = await looker_client.get_active_branch_name(LOOKER_PROJECT)
    assert active_branch == new_branch
    await manager.__aexit__()
    active_branch = await looker_client.get_active_branch_name(LOOKER_PROJECT)
    assert active_branch == starting_branch


@patch("spectacles.runner.LookerBranchManager.get_project_imports", return_value=[])
@patch("spectacles.runner.time_hash", return_value="abc123")
async def test_manage_current_branch_with_ref(
    mock_time_hash: MagicMock, mock_get_imports: AsyncMock, looker_client: LookerClient
) -> None:
    """User is on branch A, checkout branch A with a commit ref and test.

    The manager should create a new temp branch based on branch A, checkout the temp
    branch, test, checkout branch A, and delete the temp branch.

    """
    # Set up starting branch and workspace
    await looker_client.update_workspace("dev")
    starting_branch = "pytest"
    await looker_client.checkout_branch(LOOKER_PROJECT, starting_branch)
    commit = "e2d21d"

    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)

    await manager(ref=commit).__aenter__()
    assert manager.init_state.branch == starting_branch
    assert manager.is_temp_branch
    temp_branch = manager.branch
    branch_info = await looker_client.get_active_branch(LOOKER_PROJECT)
    assert branch_info["name"] == temp_branch
    assert branch_info["ref"][:6] == commit
    await manager.__aexit__()
    branch_info = await looker_client.get_active_branch(LOOKER_PROJECT)
    assert branch_info["name"] == starting_branch
    assert branch_info["ref"][:6] != commit
    all_branches = await looker_client.get_all_branches(LOOKER_PROJECT)
    assert temp_branch not in all_branches


@patch("spectacles.runner.time_hash", return_value="abc123")
async def test_manage_current_branch_with_import_projects(
    mock_time_hash: MagicMock, looker_client: LookerClient
) -> None:
    """User is on branch A, checkout branch A and test.

    We should not import any temp branches because we are staying in the production
    workspace.

    The manager should not perform any branch checkouts, just test.

    """
    # Set up workspace
    await looker_client.update_workspace("production")

    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)

    await manager().__aenter__()
    assert not manager.is_temp_branch
    starting_branch = await looker_client.get_active_branch_name(LOOKER_PROJECT)
    await manager.__aexit__()
    active_branch = await looker_client.get_active_branch_name(LOOKER_PROJECT)
    assert active_branch == starting_branch


@patch("spectacles.runner.time_hash", return_value="abc123")
async def test_manage_other_branch_with_import_projects(
    mock_time_hash: MagicMock, looker_client: LookerClient
) -> None:
    """User is on branch A, checkout branch B and test.

    The manager should checkout branch B, test, then checkout branch A.

    We are setting import projects to True. The manager should create a temp branch
    in the dependent project, and clean it up at the end.

    """
    # Set up starting branch and workspace
    await looker_client.update_workspace("dev")
    starting_branch = "pytest"
    await looker_client.checkout_branch(LOOKER_PROJECT, starting_branch)
    dependent_project = "looker-demo"

    new_branch = "pytest-additional"
    assert new_branch != starting_branch
    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)

    await manager(ref=new_branch).__aenter__()
    assert manager.init_state.branch == starting_branch
    assert not manager.is_temp_branch
    dependent_project_manager = manager.import_managers[0]
    assert dependent_project_manager.is_temp_branch
    temp_branch = dependent_project_manager.branch
    active_branch = await looker_client.get_active_branch_name(LOOKER_PROJECT)
    assert active_branch == new_branch
    active_branch = await looker_client.get_active_branch_name(dependent_project)
    assert active_branch == temp_branch
    await manager.__aexit__()
    active_branch = await looker_client.get_active_branch_name(LOOKER_PROJECT)
    assert active_branch == starting_branch
    active_branch = await looker_client.get_active_branch_name(dependent_project)
    assert active_branch == dependent_project_manager.init_state.branch
    all_branches = await looker_client.get_all_branches(dependent_project)
    assert temp_branch not in all_branches


@patch("spectacles.runner.LookerBranchManager.get_project_imports", return_value=[])
async def test_manage_prod_with_advanced_deploy(
    mock_get_imports: AsyncMock, looker_client: LookerClient
) -> None:
    # Set up starting branch and workspace
    project = "spectacles-advanced-deploy"
    await looker_client.update_workspace("production")
    branch_info = await looker_client.get_active_branch(project)
    commit = branch_info["ref"]

    manager = LookerBranchManager(looker_client, project)
    async with manager():
        assert manager.init_state.workspace == "production"
        assert manager.init_state.commit == commit
        workspace = await looker_client.get_workspace()
    assert workspace == "production"


@patch("spectacles.runner.time_hash", return_value="abc123")
async def test_manage_with_ref_import_projects(
    mock_time_hash: MagicMock, looker_client: LookerClient
) -> None:
    """User is on branch A, checkout branch B and test.

    The manager should create a new temp branch based on branch B, checkout the temp
    branch, test, checkout branch A, and delete the temp branch.

    We are setting import projects to True. The manager should create a temp branch
    in the dependent project, and clean it up at the end.

    """
    # Set up starting branch and workspace
    await looker_client.update_workspace("production")
    dependent_project = "looker-demo"
    commit = "e2d21d"

    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)
    branch_info = await looker_client.get_active_branch(LOOKER_PROJECT)
    assert branch_info["ref"][:6] != commit

    async with manager(ref=commit):
        assert manager.init_state.workspace == "production"
        assert manager.is_temp_branch
        assert manager.commit and manager.commit[:6] == commit
        branch_info = await looker_client.get_active_branch(manager.project)
        assert branch_info["ref"][:6] == commit
        for import_manager in manager.import_managers:
            branch = await looker_client.get_active_branch_name(import_manager.project)
            assert import_manager.branch == branch

    branch_info = await looker_client.get_active_branch(LOOKER_PROJECT)
    assert branch_info["ref"][:6] != commit

    await looker_client.update_workspace("dev")
    all_branches = set(await looker_client.get_all_branches(dependent_project))
    # Confirm that no temp branches still remain
    temp_branches = set(
        import_manager.branch for import_manager in manager.import_managers
    )
    assert temp_branches.isdisjoint(all_branches)


@patch("spectacles.runner.LookerBranchManager.get_project_imports", return_value=[])
@patch("spectacles.runner.time_hash", return_value="abc123")
async def test_manage_with_ref_not_present_in_local_repo(
    mock_time_hash: MagicMock,
    mock_get_imports: AsyncMock,
    remote_repo: Repository,
    looker_client: LookerClient,
) -> None:
    # Create a commit on an external branch directly on the GitHub remote
    content_file = remote_repo.get_contents("README.md", ref="master")

    if isinstance(content_file, list):
        raise TypeError(f"Expected a single ContentFile, got {type(content_file)}")

    result = remote_repo.update_file(
        content_file.path,
        message="Updating file",
        content=".",
        sha=content_file.sha,
        branch=TMP_REMOTE_BRANCH,
    )
    commit = result["commit"].sha

    manager = LookerBranchManager(looker_client, LOOKER_PROJECT)
    async with manager(ref=commit):
        assert manager.is_temp_branch
        assert manager.commit == commit
        branch_info = await looker_client.get_active_branch(LOOKER_PROJECT)
        assert branch_info["ref"] == branch_info["remote_ref"]
        assert branch_info["ref"] == commit
