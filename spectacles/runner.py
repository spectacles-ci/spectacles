from typing import List, Dict, Any, Optional, NamedTuple
from spectacles.client import LookerClient
from spectacles.validators import SqlValidator, DataTestValidator
from spectacles.utils import log_duration, time_hash
from spectacles.logger import GLOBAL_LOGGER as logger


class BranchState(NamedTuple):
    """The original state of a project before checking out a temporary branch."""

    project: str
    original_branch: str
    temp_branch: Optional[str]


class LookerBranchManager:
    def __init__(
        self,
        client: LookerClient,
        project: str,
        name: Optional[str] = None,
        remote_reset: bool = False,
        import_projects: bool = False,
        commit_ref: Optional[str] = None,
    ):
        """Context manager for Git branch checkout, creation, and deletion."""
        self.client = client
        self.project = project
        self.name = name
        self.remote_reset = remote_reset
        self.import_projects = import_projects
        self.commit_ref = commit_ref

        # Get the current branch so we can return to it afterwards
        self.original_branch = self.client.get_active_branch_name(self.project)
        # If the desired branch is master and no ref is passed, we can stay in prod
        self.workspace = "production" if name == "master" and not commit_ref else "dev"
        self.temp_branches: List[BranchState] = []

    def __enter__(self):
        self.client.update_workspace(self.project, self.workspace)
        if self.import_projects and self.name != "master":
            self.branch_imported_projects()
        if self.commit_ref:
            """Can't delete branches from the production workspace so we need to save
            the starting dev branch to return to and to use as a base to delete
            temporary branches from."""
            starting_branch = self.client.get_active_branch_name(self.project)
            temp_branch = self.setup_temp_branch(
                self.project, original_branch=starting_branch
            )
            self.client.create_branch(self.project, temp_branch)
            self.client.update_branch(self.project, temp_branch, self.commit_ref)
        # If we didn't start on the desired branch, check it out
        elif self.original_branch != self.name:
            self.client.checkout_branch(self.project, self.name)
            if self.remote_reset:
                self.client.reset_to_remote(self.project)

    def __exit__(self, *args):
        if self.temp_branches:
            # Tear down any temporary branches
            for project, original_branch, temp_branch in self.temp_branches:
                self.restore_branch(project, original_branch, temp_branch)

        # Return to the starting branch
        self.restore_branch(self.project, self.original_branch)

    def setup_temp_branch(self, project: str, original_branch: str) -> str:
        name = "tmp_spectacles_" + time_hash()
        logger.debug(
            f"Branch '{name}' will be restored to branch '{original_branch}' in "
            f"project '{project}'"
        )
        self.temp_branches.append(BranchState(project, original_branch, name))
        return name

    def restore_branch(
        self, project: str, original_branch: str, temp_branch: Optional[str] = None
    ):
        message = f"Restoring project '{project}' to branch '{original_branch}'"
        if temp_branch:
            message += f" and deleting temporary branch '{temp_branch}'"
        logger.debug(message)
        if original_branch == "master":
            self.client.update_workspace(project, "production")
        else:
            self.client.checkout_branch(project, original_branch)
        if temp_branch:
            self.client.delete_branch(project, temp_branch)

    def branch_imported_projects(self):
        logger.debug("Creating temporary branches in imported projects")
        manifest = self.client.get_manifest(self.project)
        local_dependencies = [p for p in manifest["imports"] if not p["is_remote"]]

        for project in local_dependencies:
            original_branch = self.client.get_active_branch_name(project["name"])
            temp_branch = self.setup_temp_branch(project["name"], original_branch)
            self.client.create_branch(project["name"], temp_branch)
            self.client.update_branch(project["name"], temp_branch)


class Runner:
    """Runs validations and returns JSON-style dictionaries with validation results.

    Args:
        base_url: Base URL for the Looker instance, e.g. https://mycompany.looker.com.
        project: Name of the Looker project to use.
        branch: Name of the Git branch to check out.
        client_id: Looker API client ID.
        client_secret: Looker API client secret.
        port: Desired API port to use for requests.
        api_version: Desired API version to use for requests.

    Attributes:
        client: Looker API client used for making requests.

    """

    def __init__(
        self,
        base_url: str,
        project: str,
        branch: str,
        client_id: str,
        client_secret: str,
        port: int = 19999,
        api_version: float = 3.1,
        remote_reset: bool = False,
        import_projects: bool = False,
        commit_ref: str = None,
    ):
        self.project = project
        self.import_projects = import_projects
        self.client = LookerClient(
            base_url, client_id, client_secret, port, api_version
        )
        self.branch_manager = LookerBranchManager(
            self.client,
            project,
            branch,
            remote_reset=remote_reset,
            import_projects=import_projects,
            commit_ref=commit_ref,
        )

    @log_duration
    def validate_sql(
        self,
        selectors: List[str],
        exclusions: List[str],
        mode: str = "batch",
        concurrency: int = 10,
    ) -> Dict[str, Any]:
        with self.branch_manager:
            sql_validator = SqlValidator(self.client, self.project, concurrency)
            sql_validator.build_project(selectors, exclusions)
            results = sql_validator.validate(mode)
        return results

    @log_duration
    def validate_data_tests(self) -> Dict[str, Any]:
        with self.branch_manager:
            data_test_validator = DataTestValidator(self.client, self.project)
            results = data_test_validator.validate()
        return results
