from typing import List, Dict, Any, Optional, NamedTuple
import itertools
from spectacles.client import LookerClient
from spectacles.validators import SqlValidator, DataTestValidator, ContentValidator
from spectacles.utils import time_hash
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.printer import print_header
from spectacles.types import QueryMode


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
        self.commit_ref = commit_ref
        self.name = name
        self.remote_reset = remote_reset
        self.import_projects = import_projects

        # Get the current branch so we can return to it afterwards
        self.original_branch = self.client.get_active_branch_name(self.project)
        self.temp_branches: List[BranchState] = []

    def __enter__(self):
        self.client.update_workspace(self.project, self.workspace)
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
        if self.import_projects and self.name != "master":
            self.branch_imported_projects()

    def __exit__(self, *args):
        if self.temp_branches:
            # Tear down any temporary branches
            for project, original_branch, temp_branch in self.temp_branches:
                self.restore_branch(project, original_branch, temp_branch)
        self.temp_branches = []

        # Return to the starting branch
        self.restore_branch(self.project, self.original_branch)

    @property
    def name(self) -> Optional[str]:
        return self._name

    @name.setter
    def name(self, name: Optional[str]):
        self._name = name
        # If the desired branch is master and no ref is passed, we can stay in prod
        self.workspace = (
            "production" if name == "master" and not self.commit_ref else "dev"
        )

    @property
    def ref(self) -> Optional[str]:
        if self.commit_ref:
            return self.commit_ref[:6]
        else:
            return self.name

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
        commit_ref: Optional[str] = None,
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

    def validate_sql(
        self,
        selectors: List[str],
        exclusions: List[str],
        mode: QueryMode = "batch",
        concurrency: int = 10,
    ) -> Dict[str, Any]:
        with self.branch_manager:
            validator = SqlValidator(self.client, self.project, concurrency)
            logger.info(
                "Building LookML project hierarchy for project "
                f"'{self.project}' @ {self.branch_manager.ref}"
            )
            validator.build_project(selectors, exclusions, build_dimensions=True)
            explore_count = validator.project.count_explores()
            print_header(
                f"Testing {explore_count} "
                f"{'explore' if explore_count == 1 else 'explores'} "
                f"[{mode} mode] "
                f"[concurrency = {validator.query_slots}]"
            )
            results = validator.validate(mode)
        return results

    def validate_data_tests(
        self, selectors: List[str], exclusions: List[str]
    ) -> Dict[str, Any]:
        with self.branch_manager:
            validator = DataTestValidator(self.client, self.project)
            logger.info(
                "Building LookML project hierarchy for project "
                f"'{self.project}' @ {self.branch_manager.ref}"
            )
            validator.build_project(selectors, exclusions)
            explore_count = validator.project.count_explores()
            print_header(
                f"Running data tests based on {explore_count} "
                f"{'explore' if explore_count == 1 else 'explores'}"
            )
            results = validator.validate()
        return results

    def validate_content(
        self,
        selectors: List[str],
        exclusions: List[str],
        incremental: bool = False,
        exclude_personal: bool = False,
    ) -> Dict[str, Any]:
        with self.branch_manager:
            validator = ContentValidator(self.client, self.project, exclude_personal)
            logger.info(
                "Building LookML project hierarchy for project "
                f"'{self.project}' @ {self.branch_manager.ref}"
            )
            validator.build_project(selectors, exclusions)
            explore_count = validator.project.count_explores()
            print_header(
                f"Validating content based on {explore_count} "
                f"{'explore' if explore_count == 1 else 'explores'}"
                + (" [incremental mode] " if incremental else "")
            )
            results = validator.validate()
        if incremental and self.branch_manager.name != "master":
            logger.debug("Starting another content validation against master")
            self.branch_manager.commit_ref = None
            self.branch_manager.name = "master"
            with self.branch_manager:
                logger.debug(
                    "Building LookML project hierarchy for project "
                    f"'{self.project}' @ {self.branch_manager.ref}"
                )
                validator.build_project(selectors, exclusions)
                main_results = validator.validate()
            return self._incremental_results(main=main_results, additional=results)
        else:
            return results

    @staticmethod
    def _incremental_results(
        main: Dict[str, Any], additional: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Returns a new result with only the additional errors in `additional`."""
        incremental: Dict[str, Any] = {
            "validator": "content",
            # Start with models and explores we know passed in `additional`
            "tested": [test for test in additional["tested"] if test["passed"]],
            "errors": [],
        }

        # Build a list of disputed tests where dupes by model and explore are allowed
        tests = []
        for error in additional["errors"]:
            if error in main["errors"]:
                passed = True
            else:
                passed = False
                incremental["errors"].append(error)

            test = dict(model=error["model"], explore=error["explore"], passed=passed)
            tests.append(test)

        def key_by(x):
            return (x["model"], x["explore"])

        if tests:
            # Dedupe the list of tests, grouping by model and explore and taking the min
            # To do this, we group by model and explore and sort by `passed`
            tests = sorted(tests, key=lambda x: (x["model"], x["explore"], x["passed"]))
            for key, group in itertools.groupby(tests, key=key_by):
                items = list(group)
                incremental["tested"].append(items[0])

        # Re-sort the final list
        incremental["tested"] = sorted(incremental["tested"], key=key_by)

        # Recompute the overall state of the test suite
        passed = min((test["passed"] for test in incremental["tested"]), default=True)
        incremental["status"] = "passed" if passed else "failed"

        return incremental
