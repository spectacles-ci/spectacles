from spectacles.exceptions import LookerApiError
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import itertools
from spectacles.client import LookerClient
from spectacles.validators import SqlValidator, DataTestValidator, ContentValidator
from spectacles.utils import time_hash
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.printer import print_header
from spectacles.types import QueryMode


@dataclass
class ProjectState:
    project: str
    workspace: str
    branch: str
    commit: str


class LookerBranchManager:
    def __init__(self, client: LookerClient, project: str, remote_reset: bool = False):
        """Context manager for Git branch checkout, creation, and deletion."""
        self.client = client
        self.project = project
        self.remote_reset = remote_reset

        state: ProjectState = self.get_project_state()
        self.workspace: str = state.workspace
        self.history: List[ProjectState] = [state]
        self.imports: List[str] = self.get_project_imports()

        self.commit: Optional[str] = None
        self.branch: Optional[str] = None
        self.is_temp_branch: bool = False
        self.import_managers: List[LookerBranchManager] = []

    def __call__(
        self,
        branch: Optional[str] = None,
        commit: Optional[str] = None,
        ephemeral: Optional[bool] = None,
    ):
        if branch and commit:
            raise ValueError("Cannot call with both branch and commit.")
        self.branch = branch
        self.commit = commit
        self.ephemeral = ephemeral or bool(commit)
        return self

    def __enter__(self):
        # A branch was passed, so we check it out in dev mode.
        if self.branch:
            self.update_workspace("dev")
            self.client.checkout_branch(self.project, self.branch)
            if self.remote_reset:
                self.client.reset_to_remote(self.project)
        # A commit was passed, so we non-destructively create a temporary branch we can
        # hard reset to the commit.
        elif self.commit:
            self.branch = self.checkout_temp_branch(self.commit)
        # Neither branch nor commit were passed, so go to production.
        else:
            self.update_workspace("production")
            prod_state = self.get_project_state()
            self.branch = prod_state.branch
            self.commit = prod_state.commit
            if self.ephemeral:
                self.branch = self.checkout_temp_branch(f"origin/{prod_state.branch}")

        logger.debug(
            f"Project '{self.project}' state: branch '{self.branch}' @ "
            f"{self.commit or 'HEAD'} in '{self.workspace}' workspace "
            f"[ephemeral = {self.ephemeral}]"
        )

        # Create temporary branches off production for manifest dependencies
        if self.imports and self.workspace != "production":
            logger.debug("Creating temporary branches in imported projects")
            for project in self.imports:
                manager = LookerBranchManager(self.client, project)
                manager(ephemeral=True).__enter__()
                self.import_managers.append(manager)

    def __exit__(self, *args):
        message = (
            f"Restoring project '{self.project}' to branch '{self.init_state.branch}'"
        )
        if self.is_temp_branch:
            message += f" and deleting temporary branch '{self.branch}'"
        logger.debug(message)

        if self.is_temp_branch:
            dev_state = self.history.pop()
            self.client.checkout_branch(self.project, dev_state.branch)
            self.client.delete_branch(self.project, self.branch)

        for manager in self.import_managers:
            manager.__exit__()

        if self.init_state.workspace == "production":
            self.update_workspace("production")
        else:
            self.update_workspace("dev")
            self.client.checkout_branch(self.project, self.init_state.branch)

    @property
    def init_state(self) -> ProjectState:
        return self.history[0]

    @property
    def ref(self) -> Optional[str]:
        if self.commit:
            return self.commit[:6]
        else:
            return self.branch

    def update_workspace(self, workspace: str):
        if workspace not in ("dev", "production"):
            raise ValueError("Workspace can only be set to 'dev' or 'production'")
        if self.workspace != workspace:
            self.client.update_workspace(workspace)
            self.workspace = workspace

    def get_project_state(self) -> ProjectState:
        workspace = self.client.get_workspace()
        branch_info = self.client.get_active_branch(self.project)
        return ProjectState(
            self.project, workspace, branch_info["name"], branch_info["ref"]
        )

    def get_project_imports(self) -> List[str]:
        try:
            manifest = self.client.get_manifest(self.project)
        except LookerApiError:
            return []
        else:
            return [p["name"] for p in manifest["imports"] if not p["is_remote"]]

    def checkout_temp_branch(self, ref: str) -> str:
        """Creates a temporary branch off a commit or off production."""
        # Save the dev mode state so we have somewhere to delete the temp branch
        # from later. We can't delete branches from prod workspace.
        self.update_workspace("dev")
        self.history.append(self.get_project_state())
        name = "tmp_spectacles_" + time_hash()
        logger.debug(
            f"Branching '{name}' off '{ref}'. "
            f"Afterwards, restoring to branch '{self.init_state.branch}' in "
            f"project '{self.project}'"
        )
        self.client.create_branch(self.project, name, ref)
        # self.client.hard_reset_branch(project, name, ref)
        self.is_temp_branch = True
        return name


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
        client_id: str,
        client_secret: str,
        port: int = 19999,
        api_version: float = 3.1,
        remote_reset: bool = False,
    ):
        self.project = project
        self.client = LookerClient(
            base_url, client_id, client_secret, port, api_version
        )
        self.branch_manager = LookerBranchManager(self.client, project, remote_reset)

    def validate_sql(
        self,
        branch: Optional[str],
        commit: Optional[str],
        selectors: List[str],
        exclusions: List[str],
        mode: QueryMode = "batch",
        concurrency: int = 10,
        profile: bool = False,
        runtime_threshold: int = 5,
    ) -> Dict[str, Any]:
        with self.branch_manager(branch, commit):
            validator = SqlValidator(
                self.client, self.project, concurrency, runtime_threshold
            )
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
            results = validator.validate(mode, profile)
        return results

    def validate_data_tests(
        self,
        branch: Optional[str],
        commit: Optional[str],
        selectors: List[str],
        exclusions: List[str],
    ) -> Dict[str, Any]:
        with self.branch_manager(branch, commit):
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
        branch: Optional[str],
        commit: Optional[str],
        selectors: List[str],
        exclusions: List[str],
        incremental: bool = False,
        exclude_personal: bool = False,
    ) -> Dict[str, Any]:
        with self.branch_manager(branch, commit):
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
        if incremental and self.branch_manager.workspace != "production":
            logger.debug("Starting another content validation against production")
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
