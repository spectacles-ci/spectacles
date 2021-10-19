import re
from spectacles.exceptions import LookerApiError
from typing import List, Optional, cast
from dataclasses import dataclass
import itertools
from spectacles.client import LookerClient
from spectacles.validators import (
    SqlValidator,
    DataTestValidator,
    ContentValidator,
    LookMLValidator,
)
from spectacles.types import JsonDict
from spectacles.validators.sql import SqlTest
from spectacles.exceptions import SpectaclesException
from spectacles.utils import time_hash
from spectacles.lookml import build_project, Project, Explore
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.printer import print_header, LINE_WIDTH


@dataclass
class ProjectState:
    project: str
    workspace: str
    branch: str
    commit: str


def is_commit(string: str) -> bool:
    """Tests if a string is a SHA1 hash."""
    return bool(re.match("[0-9a-f]{5,40}", string))


class LookerBranchManager:
    def __init__(self, client: LookerClient, project: str, remote_reset: bool = False):
        """Context manager for Git branch checkout, creation, and deletion."""
        logger.debug(f"Setting up branch manager in project '{project}'")
        self.client = client
        self.project = project
        self.remote_reset = remote_reset

        state: ProjectState = self.get_project_state()
        self.workspace: str = state.workspace
        self.history: List[ProjectState] = [state]
        self.imports: List[str] = self.get_project_imports()
        logger.debug(
            f"Project '{self.project}' imports the following projects: {self.imports}"
        )

        self.commit: Optional[str] = None
        self.branch: Optional[str] = None
        self.is_temp_branch: bool = False
        self.import_managers: List[LookerBranchManager] = []

    def __call__(self, ref: Optional[str] = None, ephemeral: Optional[bool] = None):
        logger.debug(
            f"\nSetting Git state for project '{self.project}' "
            f"@ {ref or 'production'}\n" + "-" * LINE_WIDTH
        )
        self.branch = None
        self.commit = None

        if ref is None:
            pass
        elif is_commit(ref):
            self.commit = ref
        else:
            self.branch = ref

        if ephemeral is None:
            if self.commit:
                self.ephemeral = True
            else:
                self.ephemeral = False
        else:
            if self.commit and ephemeral is False:
                raise ValueError(
                    "ephemeral = False is invalid for a commit reference because "
                    "it's impossible to checkout a commit directly in Looker. "
                    "You must use a temp branch."
                )

            self.ephemeral = ephemeral

        self.is_temp_branch = False
        self.import_managers = []
        return self

    def __enter__(self):
        # A branch was passed, so we check it out in dev mode.
        if self.branch:
            self.update_workspace("dev")
            if self.ephemeral:
                self.branch = self.checkout_temp_branch(self.branch)
            else:
                self.client.checkout_branch(self.project, self.branch)
                if self.remote_reset:
                    self.client.reset_to_remote(self.project)
        # A commit was passed, so we non-destructively create a temporary branch we can
        # hard reset to the commit.
        elif self.commit:
            self.branch = self.checkout_temp_branch(self.commit)
        # Neither branch nor commit were passed, so go to production.
        else:
            if self.init_state.workspace == "production":
                prod_state = self.init_state
            else:
                self.update_workspace("production")
                prod_state = self.get_project_state()
            self.branch = prod_state.branch
            self.commit = prod_state.commit
            if self.ephemeral:
                self.branch = self.checkout_temp_branch(prod_state.commit)

        logger.debug(
            f"Set project '{self.project}' to branch '{self.branch}' @ "
            f"{(self.commit or 'HEAD')[:6]} in {self.workspace} workspace "
            f"[ephemeral = {self.ephemeral}]"
        )

        # Create temporary branches off production for manifest dependencies
        if not self.imports:
            logger.debug(f"Project '{self.project}' doesn't import any other projects")
        elif self.workspace == "production":
            logger.debug(
                "In production, no need for temporary branches in imported projects"
            )
        else:
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
        self.client.create_branch(self.project, name)
        self.client.hard_reset_branch(self.project, name, ref)
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
        client: LookerClient,
        project: str,
        remote_reset: bool = False,
    ):
        self.project = project
        self.client = client
        self.branch_manager = LookerBranchManager(client, project, remote_reset)

    def validate_sql(
        self,
        ref: Optional[str] = None,
        filters: List[str] = None,
        fail_fast: bool = True,
        incremental: bool = False,
        target: Optional[str] = None,
        concurrency: int = 10,
        profile: bool = False,
        runtime_threshold: int = 5,
    ) -> JsonDict:
        if filters is None:
            filters = ["*/*"]
        validator = SqlValidator(self.client, concurrency, runtime_threshold)
        tests: List[SqlTest] = []

        # Create explore-level tests for the desired ref
        with self.branch_manager(ref=ref, ephemeral=incremental):
            base_ref = self.branch_manager.ref  # Resolve the full ref after checkout
            logger.debug("Building explore tests for the desired ref")
            project = build_project(
                self.client, name=self.project, filters=filters, include_dimensions=True
            )
            base_tests = validator.create_tests(project, compile_sql=incremental)

        if incremental:
            unique_base_tests = set(base_tests)
            # Create explore tests for the target
            with self.branch_manager(ref=target, ephemeral=True):
                target_ref = self.branch_manager.ref
                if target_ref == base_ref:
                    raise SpectaclesException(
                        name="incremental-same-ref",
                        title="Incremental comparison to the same Git state",
                        detail=(
                            f"The base ref ({ref or 'production'}) and "
                            f"target ref ({target or 'production'}) point to the "
                            f"same commit ({base_ref}), "
                            "so incremental comparison isn't possible."
                        ),
                    )
                logger.debug("Building explore tests for the target ref")
                target_project: Project = build_project(
                    self.client,
                    name=self.project,
                    filters=filters,
                    include_dimensions=True,
                )
                target_tests = validator.create_tests(target_project, compile_sql=True)
                unique_target_tests = set(target_tests)

            # Determine which explore tests are identical between target and base
            intersection = unique_base_tests & unique_target_tests
            logger.debug(
                f"Found {len(unique_base_tests - unique_target_tests)} "
                "explore tests with unique SQL"
            )

            # Mark explores with the same compiled SQL (test) as skipped
            for test in intersection:
                explore = cast(Explore, test.lookml_ref)  # Appease mypy
                explore.skipped = True

            # Test explores with unique SQL for base ref
            tests = list(unique_base_tests - unique_target_tests)
        else:
            tests = base_tests

        explore_count = project.count_explores()
        print_header(
            f"Testing {explore_count} "
            f"{'explore' if explore_count == 1 else 'explores'} "
            + ("[fail fast] " if fail_fast else "")
            + f"[concurrency = {validator.query_slots}]"
        )

        with self.branch_manager(ref=ref):
            validator.run_tests(tests, profile)

        # Create dimension tests for the desired ref when explores errored
        if not fail_fast:
            with self.branch_manager(ref=ref, ephemeral=incremental):
                base_ref = self.branch_manager.ref
                logger.debug("Building dimension tests for the desired ref")
                base_tests = validator.create_tests(
                    project, compile_sql=incremental, at_dimension_level=True
                )

        # Create dimension tests for the target ref
        if incremental:
            with self.branch_manager(ref=target, ephemeral=True):
                target_ref = self.branch_manager.ref
                logger.debug("Building dimension tests for the target ref")
                target_tests = validator.create_tests(
                    target_project, compile_sql=True, at_dimension_level=True
                )

            # Keep only the dimension tests that don't exist on the target branch
            logger.debug(
                f"Removing tests that would exist in project @ {target or 'production'}"
            )
            tests = list(set(base_tests) - set(target_tests))
            logger.debug(
                f"{len(tests)} tests found @ '{target_ref}' "
                f"that are not present @ '{base_ref}'"
            )
        else:
            tests = base_tests

        with self.branch_manager(ref=ref):
            validator.run_tests(tests, profile)

        results = project.get_results(validator="sql", fail_fast=fail_fast)
        return results

    def validate_data_tests(
        self,
        ref: Optional[str] = None,
        filters: List[str] = None,
    ) -> JsonDict:
        if filters is None:
            filters = ["*/*"]
        with self.branch_manager(ref):
            validator = DataTestValidator(self.client)
            logger.info(
                "Building LookML project hierarchy for project "
                f"'{self.project}' @ {self.branch_manager.ref}"
            )
            project = build_project(self.client, name=self.project, filters=filters)
            explore_count = project.count_explores()
            print_header(
                f"Running data tests based on {explore_count} "
                f"{'explore' if explore_count == 1 else 'explores'}"
            )
            tests = validator.get_tests(project)
            validator.validate(tests)

        results = project.get_results(validator="data_test")
        return results

    def validate_lookml(
        self, branch: Optional[str], commit: Optional[str], severity: str
    ) -> Dict[str, Any]:
        with self.branch_manager(branch, commit):
            validator = LookMLValidator(self.client, self.project)
            print_header(f"Validating LookML in project {self.project} [{severity}]")
            results = validator.validate(severity)
        return results

    def validate_content(
        self,
        ref: Optional[str] = None,
        filters: List[str] = None,
        incremental: bool = False,
        target: Optional[str] = None,
        exclude_personal: bool = False,
        exclude_folders: List[int] = None,
        include_folders: List[int] = None,
    ) -> JsonDict:
        if filters is None:
            filters = ["*/*"]
        if exclude_folders is None:
            exclude_folders = []
        if include_folders is None:
            include_folders = []

        with self.branch_manager(ref=ref):
            validator = ContentValidator(
                self.client,
                exclude_personal,
                exclude_folders,
                include_folders,
            )
            logger.info(
                "Building LookML project hierarchy for project "
                f"'{self.project}' @ {self.branch_manager.ref}"
            )
            project = build_project(self.client, name=self.project, filters=filters)
            explore_count = project.count_explores()
            print_header(
                f"Validating content based on {explore_count} "
                f"{'explore' if explore_count == 1 else 'explores'}"
                + (" [incremental mode] " if incremental else "")
            )
            validator.validate(project)
            results = project.get_results(validator="content")

        if incremental and (self.branch_manager.branch or self.branch_manager.commit):
            logger.debug("Starting another content validation against production")
            with self.branch_manager(ref=target):
                logger.debug(
                    "Building LookML project hierarchy for project "
                    f"'{self.project}' @ {self.branch_manager.ref}"
                )
                target_project = build_project(
                    self.client, name=self.project, filters=filters
                )
                validator.validate(target_project)
                target_results = project.get_results(validator="content")

            return self._incremental_results(main=target_results, additional=results)
        else:
            return results

    @staticmethod
    def _incremental_results(main: JsonDict, additional: JsonDict) -> JsonDict:
        """Returns a new result with only the additional errors in `additional`."""
        incremental: JsonDict = {
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
