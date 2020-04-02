from typing import List, Callable
import functools
from spectacles.client import LookerClient
from spectacles.validators import SqlValidator, DataTestValidator
from spectacles.lookml import Project
from spectacles.exceptions import DataTestError
from spectacles.utils import log_duration, time_hash


def manage_dependent_branches(fn: Callable) -> Callable:
    functools.wraps(fn)

    def wrapper(self, *args, **kwargs):
        if self.manifest_dependency:
            manifest = self.client.get_manifest(self.project)

            local_dependencies = [p for p in manifest["imports"] if not p["is_remote"]]

            for project in local_dependencies:
                project["active_branch"] = self.client.get_active_branch(
                    project["name"]
                )
                project["temp_branch"] = "tmp_spectacles_" + time_hash()
                self.client.create_branch(project["name"], project["temp_branch"])

            fn(self, *args, **kwargs)

            for project in local_dependencies:
                self.client.update_session(project["name"], project["active_branch"])
                self.client.delete_branch(project["name"], project["temp_branch"])

        else:
            fn(self, *args, **kwargs)

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
        manifest_dependency: bool = False,
    ):
        self.project = project
        self.manifest_dependency = manifest_dependency
        self.client = LookerClient(
            base_url, client_id, client_secret, port, api_version
        )
        self.client.update_session(project, branch, remote_reset)

    @manage_dependent_branches
    @log_duration
    def validate_sql(
        self, selectors: List[str], mode: str = "batch", concurrency: int = 10
    ) -> Project:
        sql_validator = SqlValidator(self.client, self.project, concurrency)
        sql_validator.build_project(selectors)
        project = sql_validator.validate(mode)
        return project

    @manage_dependent_branches
    @log_duration
    def validate_data_tests(self) -> List[DataTestError]:
        data_test_validator = DataTestValidator(self.client, self.project)
        errors = data_test_validator.validate()
        return errors
