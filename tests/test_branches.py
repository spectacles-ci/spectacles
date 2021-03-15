import os
from spectacles.client import LookerClient
import pytest
from github import Github as GitHub, Repository

LOOKER_PROJECT = "eye_exam"
TEST_BRANCH_NAME = "pytest-tmp"


@pytest.fixture(scope="session")
def repo():
    access_token = os.environ["GITHUB_ACCESS_TOKEN"]
    client = GitHub(access_token)
    repo = client.get_repo("spectacles-ci/eye-exam")
    return repo


@pytest.fixture
def test_branch(repo):
    """Creates a test branch off master, waits for test execution, then cleans up."""
    branch = repo.get_branch("master")
    repo.create_git_ref(ref=f"refs/heads/{TEST_BRANCH_NAME}", sha=branch.commit.sha)
    yield
    repo.get_git_ref(f"heads/{TEST_BRANCH_NAME}").delete()


# I should be able to check out another branch that I didn't create
# The commit ref should match that branch on remote
@pytest.mark.vcr
def test_checkout_branch_not_initially_in_local(
    repo: Repository, test_branch: None, looker_client: LookerClient
):
    # Add a commit on GitHub
    content = repo.get_contents("README.md", ref="master")
    result = repo.update_file(
        content.path,
        message="Updating file",
        content=".",
        sha=content.sha,
        branch=TEST_BRANCH_NAME,
    )
    commit = result["commit"].sha
    # Check out that branch from Looker client
    looker_client.update_workspace(LOOKER_PROJECT, workspace="dev")
    # Perform what's essentially a forced pull
    looker_client.hard_reset_branch(
        LOOKER_PROJECT, branch=TEST_BRANCH_NAME, ref=f"origin/{TEST_BRANCH_NAME}"
    )
    branch_info = looker_client.get_active_branch(LOOKER_PROJECT)
    print(
        f"On branch {branch_info['name']}, with ref {branch_info['ref'][:6]}, "
        f"remote ref {branch_info['remote_ref'][:6]}. "
        f"+{branch_info['ahead_count']}/-{branch_info['behind_count']} "
        "compared to remote."
    )
    # Verify HEAD is as expected
    assert branch_info["ref"] == branch_info["remote_ref"]
    assert branch_info["ref"] == commit


# I should be able to create a branch off a commit that doesn't exist on my local branch
@pytest.mark.vcr
def test_branch_off_commit_not_on_my_current_branch(
    repo: Repository, test_branch: None, looker_client: LookerClient
):
    content = repo.get_contents("README.md", ref="master")
    result = repo.update_file(
        content.path,
        message="Updating file",
        content=".",
        sha=content.sha,
        branch=TEST_BRANCH_NAME,
    )
    commit = result["commit"].sha
    # Check out that branch from Looker client
    looker_client.update_workspace(LOOKER_PROJECT, workspace="dev")
    looker_client.checkout_branch("eye_exam", "dev-josh-temple-8m75")
    # Perform what's essentially a forced pull
    looker_client.hard_reset_branch(
        LOOKER_PROJECT, branch="dev-josh-temple-8m75", ref=commit
    )
    branch_info = looker_client.get_active_branch(LOOKER_PROJECT)
    print(
        f"On branch {branch_info['name']}, with ref {branch_info['ref'][:6]}, "
        f"remote ref {branch_info['remote_ref'][:6]}. "
        f"+{branch_info['ahead_count']}/-{branch_info['behind_count']} "
        "compared to remote."
    )
    # Verify HEAD is as expected
    assert branch_info["ref"] == branch_info["remote_ref"]
    assert branch_info["ref"] == commit
