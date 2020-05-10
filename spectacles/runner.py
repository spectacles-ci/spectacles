from typing import List, Dict, Any, Callable, Optional
import functools
from spectacles.client import LookerClient
from spectacles.validators import SqlValidator, DataTestValidator
from spectacles.utils import log_duration, time_hash


def manage_dependent_branches(fn: Callable) -> Callable:
    functools.wraps(fn)

    def wrapper(self, *args, **kwargs):
        if self.import_projects:
            manifest = self.client.get_manifest(self.project)

            local_dependencies = [p for p in manifest["imports"] if not p["is_remote"]]

            for project in local_dependencies:
                project["active_branch"] = self.client.get_active_branch(
                    project["name"]
                )
                project["temp_branch"] = "tmp_spectacles_" + time_hash()
                self.client.create_branch(project["name"], project["temp_branch"])
                self.client.update_branch(project["name"], project["temp_branch"])

            response = fn(self, *args, **kwargs)

            for project in local_dependencies:
                self.client.checkout_branch(project["name"], project["active_branch"])
                self.client.delete_branch(project["name"], project["temp_branch"])

        else:
            response = fn(self, *args, **kwargs)

        return response

    return wrapper


def cleanup_temp_branches(fn: Callable) -> Callable:
    functools.wraps(fn)

    def wrapper(self, *args, **kwargs):
<<<<<<< HEAD
        try:
            response = fn(self, *args, **kwargs)
        finally:
            if self.temp_branch:
                self.client.checkout_branch(self.project, self.original_branch)
                self.client.delete_branch(self.project, self.temp_branch)
=======
        response = fn(self, *args, **kwargs)
        if self.temp_branch:
            self.client.checkout_branch(self.project, self.original_branch)
            self.client.delete_branch(self.project, self.temp_branch)
>>>>>>> 8622a67... commit ref functionality and cleanup temp branches functionality
        return response

    return wrapper


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
        self.temp_branch: Optional[str] = None
        self.client = LookerClient(
            base_url, client_id, client_secret, port, api_version
        )
        if branch == "master":
            self.client.update_workspace(project, "production")
        elif commit_ref:
            self.client.update_workspace(project, "dev")
            self.temp_branch = "tmp_spectacles_" + time_hash()
            self.original_branch = self.client.get_active_branch(project)
            self.client.create_branch(project, self.temp_branch)
            self.client.update_branch(project, self.temp_branch, commit_ref)
        else:
            self.client.update_workspace(project, "dev")
            self.client.checkout_branch(project, branch)
            if remote_reset:
                self.client.reset_to_remote(project)

    @cleanup_temp_branches
    @manage_dependent_branches
    @log_duration
    def validate_sql(
        self,
        selectors: List[str],
        exclusions: List[str],
        mode: str = "batch",
        concurrency: int = 10,
    ) -> Dict[str, Any]:
        sql_validator = SqlValidator(self.client, self.project, concurrency)
        sql_validator.build_project(selectors, exclusions)
        results = sql_validator.validate(mode)
        return results

    @cleanup_temp_branches
    @manage_dependent_branches
    @log_duration
    def validate_data_tests(self) -> Dict[str, Any]:
        data_test_validator = DataTestValidator(self.client, self.project)
        results = data_test_validator.validate()
        return results
