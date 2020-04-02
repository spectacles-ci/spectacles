from typing import List, Dict, Any
import backoff  # type: ignore
import requests
from requests.exceptions import Timeout
import spectacles.utils as utils
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.exceptions import SpectaclesException, ApiConnectionError

JsonDict = Dict[str, Any]
TIMEOUT_SEC = 300


class LookerClient:
    """Wraps some endpoints of the Looker API, issues requests and handles responses.

    Args:
        base_url: Base URL for the Looker instance, e.g. https://mycompany.looker.com.
        client_id: Looker API client ID.
        client_secret: Looker API client secret.
        port: Desired API port to use for requests.
        api_version: Desired API version to use for requests.

    Attributes:
        api_url: Combined URL used as a base for request building.
        session: Persistent session to avoid re-authenticating.

    """

    def __init__(
        self,
        base_url: str,
        client_id: str,
        client_secret: str,
        port: int = 19999,
        api_version: float = 3.1,
    ):
        supported_api_versions = [3.1]
        if api_version not in supported_api_versions:
            raise SpectaclesException(
                f"API version {api_version} is not supported. "
                "Please use one of these supported versions instead: "
                f"{', '.join(str(ver) for ver in sorted(supported_api_versions))}"
            )

        self.base_url: str = base_url.rstrip("/")
        self.api_url: str = f"{self.base_url}:{port}/api/{api_version}/"
        self.session: requests.Session = requests.Session()

        self.authenticate(client_id, client_secret, api_version)

    def authenticate(
        self, client_id: str, client_secret: str, api_version: float
    ) -> None:
        """Logs in to Looker's API using a client ID/secret pair and an API version.

        Args:
            client_id: Looker API client ID.
            client_secret: Looker API client secret.
            api_version: Desired API version to use for requests.

        """
        logger.debug("Authenticating Looker API credentials")

        url = utils.compose_url(self.api_url, path=["login"])
        body = {"client_id": client_id, "client_secret": client_secret}
        response = self.session.post(url=url, data=body, timeout=TIMEOUT_SEC)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            details = utils.details_from_http_error(response)
            raise ApiConnectionError(
                f"Failed to authenticate to {url}\n"
                f"Attempted authentication with client ID {client_id}\n"
                f"Looker API error encountered: {error}\n"
                + "Message received from Looker's API: "
                f'"{details}"'
            )

        access_token = response.json()["access_token"]
        self.session.headers = {"Authorization": f"token {access_token}"}

        looker_version = self.get_looker_release_version()
        logger.info(
            f"Connected to Looker version {looker_version} "
            f"using Looker API {api_version}"
        )

    def get_looker_release_version(self) -> str:
        """Gets the version number of connected Looker instance.

        Returns:
            str: Looker instance release version number (e.g. 6.22.12)

        """
        logger.debug("Checking Looker instance release version")

        url = utils.compose_url(self.api_url, path=["versions"])

        response = self.session.get(url=url, timeout=TIMEOUT_SEC)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            details = utils.details_from_http_error(response)
            raise ApiConnectionError(
                "Failed to get Looker instance release version\n"
                f"Looker API error encountered: {error}\n"
                + "Message received from Looker's API: "
                f'"{details}"'
            )

        return response.json()["looker_release_version"]

    def update_session(
        self, project: str, branch: str, remote_reset: bool = False
    ) -> None:
        """Switches to a development mode session and checks out the desired branch.

        Args:
            project: Name of the Looker project to use.
            branch: Name of the Git branch to check out.

        """
        if branch == "master":
            logger.debug("Updating session to use production workspace")
            url = utils.compose_url(self.api_url, path=["session"])
            body = {"workspace_id": "production"}
            response = self.session.patch(url=url, json=body, timeout=TIMEOUT_SEC)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as error:
                details = utils.details_from_http_error(response)
                raise ApiConnectionError(
                    f"Unable to update session to production workspace.\n"
                    f"Looker API error encountered: {error}\n"
                    + "Message received from Looker's API: "
                    f'"{details}"'
                )

        else:
            logger.debug("Updating session to use development workspace")
            url = utils.compose_url(self.api_url, path=["session"])
            body = {"workspace_id": "dev"}
            response = self.session.patch(url=url, json=body, timeout=TIMEOUT_SEC)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as error:
                details = utils.details_from_http_error(response)
                raise ApiConnectionError(
                    f"Unable to update session to development workspace.\n"
                    f"Looker API error encountered: {error}\n"
                    + "Message received from Looker's API: "
                    f'"{details}"'
                )

            logger.debug(f"Setting Git branch to {branch}")
            url = utils.compose_url(
                self.api_url, path=["projects", project, "git_branch"]
            )
            body = {"name": branch}
            response = self.session.put(url=url, json=body, timeout=TIMEOUT_SEC)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as error:
                details = utils.details_from_http_error(response)
                raise ApiConnectionError(
                    f"Unable to checkout Git branch {branch}. "
                    "If you have uncommitted changes on the current branch, "
                    "please commit or revert them, then try again.\n\n"
                    f"Looker API error encountered: {error}\n"
                    + "Message received from Looker's API: "
                    f'"{details}"'
                )

            if remote_reset:
                logger.debug(f"Resetting branch {branch} to remote.")
                url = utils.compose_url(
                    self.api_url, path=["projects", project, "reset_to_remote"]
                )
                response = self.session.post(url=url, timeout=TIMEOUT_SEC)
                try:
                    response.raise_for_status()
                except requests.exceptions.HTTPError as error:
                    details = utils.details_from_http_error(response)
                    raise ApiConnectionError(
                        f"Unable to reset branch to remote.\n"
                        f"Looker API error encountered: {error}\n"
                        + "Message received from Looker's API: "
                        f'"{details}"'
                    )

            logger.info(f"Checked out branch {branch}")

    def get_manifest(self, project: str) -> JsonDict:
        """Gets all the dependent LookML projects defined in the manifest file.

        Args:
            project: Name of the Looker project to use.

        Returns:
            List[JsonDict]: JSON response containing all dependent projects
        """
        logger.debug(f"Getting manifest details")
        url = utils.compose_url(self.api_url, path=["projects", project, "manifest"])
        response = self.session.get(url=url, timeout=TIMEOUT_SEC)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise ApiConnectionError(
                f"Failed to retrieve manifest for project {project}\n"
                f"Make sure you have a 'manifest.lkml' file in your project"
                f'Error raised: "{error}"'
            )

        manifest = response.json()

        print(manifest)

        return manifest

    def get_active_branch(self, project: str) -> str:
        """Gets the active branch for the user in the given project.

        Args:
            project: Name of the Looker project to use.

        Returns:
            str: Name of the active branch
        """
        logger.debug(f"Getting active branch for project {project}")
        url = utils.compose_url(self.api_url, path=["projects", project, "git_branch"])
        response = self.session.get(url=url, timeout=TIMEOUT_SEC)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise ApiConnectionError(
                f"Unable to get active branch for project {project}\n"
                f'Error raised: "{error}"'
            )

        branch_name = response.json()["name"]

        return branch_name

    def create_branch(self, project: str, branch: str):
        """Creates a branch in the given project.

        Args:
            project: Name of the Looker project to use.
            branch: Name of the branch to create.
        """
        logger.debug(f"Creating branch {branch} on project {project}")

        body = {"name": branch, "ref": "master"}
        url = utils.compose_url(self.api_url, path=["projects", project, "git_branch"])
        response = self.session.post(url=url, json=body, timeout=TIMEOUT_SEC)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise ApiConnectionError(
                f"Failed to create branch in project {project}\n"
                f'Error raised: "{error}"'
            )

    def delete_branch(self, project: str, branch: str):
        """Deletes a branch in the given project.

        Args:
            project: Name of the Looker project to use.
            branch: Name of the branch to delete.
        """
        logger.debug(f"Deleting branch {branch} in project {project}")

        url = utils.compose_url(
            self.api_url, path=["projects", project, "git_branch", branch]
        )
        response = self.session.delete(url=url, timeout=TIMEOUT_SEC)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise ApiConnectionError(
                f"Failed to delete branch {branch} in project {project}\n"
                f'Error raised: "{error}"'
            )

    def all_lookml_tests(self, project: str) -> List[JsonDict]:
        """Gets all LookML/data tests for a given project.

        Args:
            project: Name of the Looker project to use

        Returns:
            List[JsonDict]: JSON response containing all LookML/data tests

        """
        logger.debug(f"Getting LookML tests for project {project}")
        url = utils.compose_url(
            self.api_url, path=["projects", project, "lookml_tests"]
        )
        response = self.session.get(url=url, timeout=TIMEOUT_SEC)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise ApiConnectionError(
                f"Failed to retrieve data tests for project {project}\n"
                f'Error raised: "{error}"'
            )

        return response.json()

    def run_lookml_test(self, project: str, model: str = None) -> List[JsonDict]:
        """Runs all LookML/data tests for a given project and model (optional)

        This command only runs tests in production, as the Looker API doesn't currently
        allow us to run data tests on a specific branch.

        Args:
            project: Name of the Looker project to use
            model: Optional name of the LookML model to restrict testing to

        Returns:
            List[JsonDict]: JSON response containing any LookML/data test errors

        """
        logger.debug(f"Running LookML tests for project {project}")
        url = utils.compose_url(
            self.api_url, path=["projects", project, "lookml_tests", "run"]
        )
        if model is not None:
            response = self.session.get(
                url=url, params={"model": model}, timeout=TIMEOUT_SEC
            )
        else:
            response = self.session.get(url=url, timeout=TIMEOUT_SEC)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise ApiConnectionError(
                f"Failed to run data tests for project {project}\n"
                f'Error raised: "{error}"'
            )

        return response.json()

    def get_lookml_models(self) -> List[JsonDict]:
        """Gets all models and explores from the LookmlModel endpoint.

        Returns:
            List[JsonDict]: JSON response containing LookML models and explores.

        """
        logger.debug(f"Getting all models and explores from {self.base_url}")
        url = utils.compose_url(self.api_url, path=["lookml_models"])
        response = self.session.get(url=url, timeout=TIMEOUT_SEC)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            details = utils.details_from_http_error(response)
            raise ApiConnectionError(
                f"Unable to retrieve explores.\n"
                f"Looker API error encountered: {error}\n"
                + "Message received from Looker's API: "
                f'"{details}"'
            )

        return response.json()

    def get_lookml_dimensions(self, model: str, explore: str) -> List[str]:
        """Gets all dimensions for an explore from the LookmlModel endpoint.

        Args:
            model: Name of LookML model to query.
            explore: Name of LookML explore to query.

        Returns:
            List[str]: Names of all the dimensions in the specified explore. Dimension
                names are returned in the format 'explore_name.dimension_name'.

        """
        logger.debug(f"Getting all dimensions from explore {explore}")
        url = utils.compose_url(
            self.api_url, path=["lookml_models", model, "explores", explore]
        )
        response = self.session.get(url=url, timeout=TIMEOUT_SEC)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            details = utils.details_from_http_error(response)
            raise ApiConnectionError(
                f'Unable to get dimensions for explore "{explore}".\n'
                f"Looker API error encountered: {error}\n"
                + "Message received from Looker's API: "
                f'"{details}"'
            )

        return response.json()["fields"]["dimensions"]

    @backoff.on_exception(backoff.expo, (Timeout,), max_tries=2)
    def create_query(self, model: str, explore: str, dimensions: List[str]) -> int:
        """Creates a Looker async query for one or more specified dimensions.

        The query created is a SELECT query, selecting all dimensions specified for a
        certain model and explore. Looker builds the query using the `sql` field in the
        LookML for each dimension.

        If a Timeout exception is received, attempts to retry.

        """
        # Using old-style string formatting so that strings are formatted lazily
        logger.debug(
            "Creating async query for %s/%s/%s",
            model,
            explore,
            "*" if len(dimensions) != 1 else dimensions[0],
        )
        body = {
            "model": model,
            "view": explore,
            "fields": dimensions,
            "limit": 0,
            "filter_expression": "1=2",
        }
        url = utils.compose_url(self.api_url, path=["queries"])
        response = self.session.post(url=url, json=body, timeout=TIMEOUT_SEC)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise ApiConnectionError(
                f"Failed to run create query for {model}/{explore}/"
                f'{"*" if len(dimensions) > 1 else dimensions[0]}\n'
                f'Error raised: "{error}"'
            )
        result = response.json()

        query_id = result["id"]
        logger.debug(
            "Query for %s/%s/%s created as query %d",
            model,
            explore,
            "*" if len(dimensions) != 1 else dimensions[0],
            query_id,
        )
        return query_id

    @backoff.on_exception(backoff.expo, (Timeout,), max_tries=2)
    def create_query_task(self, query_id: int) -> str:
        """Runs a previously created query asynchronously and returns the query task ID.

        If a ClientError or TimeoutError is received, attempts to retry.

        Args:
            session: Existing asychronous HTTP session.
            query_id: ID of a previously created query to run.

        Returns:
            str: ID for the query task, used to check on the status of the query, which
                is being run asynchronously.

        """
        # Using old-style string formatting so that strings are formatted lazily
        logger.debug("Starting query %d", query_id)
        body = {"query_id": query_id, "result_format": "json_detail"}
        url = utils.compose_url(self.api_url, path=["query_tasks"])

        response = self.session.post(
            url=url, json=body, params={"cache": "false"}, timeout=TIMEOUT_SEC
        )
        response.raise_for_status()
        result = response.json()
        query_task_id = result["id"]
        logger.debug("Query %d is running under query task %s", query_id, query_task_id)
        return query_task_id

    def get_query_task_multi_results(self, query_task_ids: List[str]) -> JsonDict:
        """Returns query task results.

        If a ClientError or TimeoutError is received, attempts to retry.

        Args:
            query_task_ids: IDs for the query tasks running asynchronously.

        Returns:
            List[JsonDict]: JSON response from the query task.

        """
        # Using old-style string formatting so that strings are formatted lazily
        logger.debug(
            "Attempting to get results for %d query tasks", len(query_task_ids)
        )
        url = utils.compose_url(self.api_url, path=["query_tasks", "multi_results"])
        response = self.session.get(
            url=url,
            params={"query_task_ids": ",".join(query_task_ids)},
            timeout=TIMEOUT_SEC,
        )
        response.raise_for_status()
        result = response.json()
        return result

    def cancel_query_task(self, query_task_id: str):
        """Cancels a query task.

        Args:
            query_task_id: ID for the query task to cancel.

        """
        logger.debug(f"Cancelling query task: {query_task_id}")
        url = utils.compose_url(self.api_url, path=["running_queries", query_task_id])
        self.session.delete(url=url, timeout=TIMEOUT_SEC)

        # No raise_for_status() here because Looker API seems to give a 404
        # if you try to cancel a finished query which can happen as part of cleanup
