from typing import List, Dict, Any
import backoff  # type: ignore
import requests
from requests.exceptions import Timeout
import spectacles.utils as utils
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.exceptions import SpectaclesException, LookerApiError

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
                name="unsupported-api-version",
                title="Specified API version is not supported.",
                detail=(
                    f"Version '{api_version}' is not supported. "
                    "Please use one of these supported versions instead: "
                    f"{', '.join(str(ver) for ver in sorted(supported_api_versions))}"
                ),
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
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-authenticate",
                title="Couldn't authenticate to the Looker API.",
                status=response.status_code,
                detail=(
                    f"Unable to authenticate with client ID '{client_id}'. "
                    "Check that your credentials are correct and try again."
                ),
                response=response,
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
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-get-version",
                title="Couldn't get Looker's release version.",
                status=response.status_code,
                detail=(
                    f"Unable to get the release version of your Looker instance. "
                    "Please try again."
                ),
                response=response,
            )

        return response.json()["looker_release_version"]

    def update_workspace(self, project: str, workspace: str) -> None:
        """Updates the session workspace.

        Args:
            project: Name of the Looker project to use.
            workspace: The workspace to switch to, either 'production' or 'dev'
        """
        logger.debug(f"Updating session to use the {workspace} workspace")
        url = utils.compose_url(self.api_url, path=["session"])
        body = {"workspace_id": workspace}
        response = self.session.patch(url=url, json=body, timeout=TIMEOUT_SEC)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-update-workspace",
                title="Couldn't update the session's workspace.",
                status=response.status_code,
                detail=(
                    f"Unable to update workspace to '{workspace}'. "
                    "If you have any unsaved work on the branch "
                    "checked out by the user whose API credentials "
                    "Spectacles is using, please save it and try again."
                ),
                response=response,
            )

    def get_all_branches(self, project: str) -> List[str]:
        """Returns a list of git branches in the project repository.

        Args:
            project: Name of the Looker project to use.
        """
        logger.debug(f"Getting all Git branches in project '{project}'")
        url = utils.compose_url(
            self.api_url, path=["projects", project, "git_branches"]
        )
        response = self.session.get(url=url, timeout=TIMEOUT_SEC)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-get-branches",
                title="Couldn't get all Git branches.",
                status=response.status_code,
                detail=(
                    f"Unable to get all Git branches in project '{project}'. "
                    "Please try again."
                ),
                response=response,
            )

        return [branch["name"] for branch in response.json()]

    def checkout_branch(self, project: str, branch: str) -> None:
        """Checks out a new git branch. Only works in dev workspace.

        Args:
            project: Name of the Looker project to use.
            branch: Name of the Git branch to check out.
        """
        logger.debug(f"Setting Git branch to '{branch}'")
        url = utils.compose_url(self.api_url, path=["projects", project, "git_branch"])
        body = {"name": branch}
        response = self.session.put(url=url, json=body, timeout=TIMEOUT_SEC)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-checkout-branch",
                title="Couldn't checkout Git branch.",
                status=response.status_code,
                detail=(
                    f"Unable to checkout Git branch '{branch}'. "
                    "If you have uncommitted changes on the current branch, "
                    "please commit or revert them, then try again."
                ),
                response=response,
            )

    def reset_to_remote(self, project: str) -> None:
        """Reset a project development branch to the revision of the project that is on the remote.

        Args:
            project: Name of the Looker project to use.

        """
        logger.debug(f"Resetting branch to remote.")
        url = utils.compose_url(
            self.api_url, path=["projects", project, "reset_to_remote"]
        )
        response = self.session.post(url=url, timeout=TIMEOUT_SEC)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-reset-remote",
                title="Couldn't checkout Git branch.",
                status=response.status_code,
                detail=(
                    f"Unable to reset local Git branch"
                    "to match remote. Please try again."
                ),
                response=response,
            )

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
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-get-manifest",
                title="Couldn't retrieve project manifest.",
                status=response.status_code,
                detail=(
                    f"Failed to retrieve manifest for project '{project}'. "
                    "Make sure you have a 'manifest.lkml' file in your project, "
                    "then try again."
                ),
                response=response,
            )

        manifest = response.json()

        return manifest

    def get_active_branch(self, project: str) -> JsonDict:
        """Gets the active branch for the user in the given project.

        Args:
            project: Name of the Looker project to use.

        Returns:
            str: Name of the active branch
        """
        logger.debug(f"Getting active branch for project '{project}'")
        url = utils.compose_url(self.api_url, path=["projects", project, "git_branch"])
        response = self.session.get(url=url, timeout=TIMEOUT_SEC)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-get-active-branch",
                title="Couldn't determine active Git branch.",
                status=response.status_code,
                detail=(
                    f"Unable to get active branch for project '{project}'. "
                    "Please try again."
                ),
                response=response,
            )

        branch_name = response.json()["name"]
        logger.debug(f"The active branch is '{branch_name}'")

        return response.json()

    def get_active_branch_name(self, project: str) -> str:
        """Helper method to return only the branch name."""
        full_response = self.get_active_branch(project)
        return full_response["name"]

    def create_branch(self, project: str, branch: str, ref: str = "origin/master"):
        """Creates a branch in the given project.

        Args:
            project: Name of the Looker project to use.
            branch: Name of the branch to create.
            ref: The ref to create the branch from.
        """
        logger.debug(
            f"Creating branch '{branch}' on project '{project}' with ref '{ref}'"
        )

        body = {"name": branch, "ref": ref}
        url = utils.compose_url(self.api_url, path=["projects", project, "git_branch"])
        response = self.session.post(url=url, json=body, timeout=TIMEOUT_SEC)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-create-branch",
                title="Couldn't create new Git branch.",
                status=response.status_code,
                detail=(
                    f"Unable to create branch '{branch}' "
                    f"in project '{project}' using ref '{ref}'. "
                    "Confirm the branch doesn't already exist and try again."
                ),
                response=response,
            )

    def update_branch(self, project: str, branch: str, ref: str = "origin/master"):
        """Updates a branch to the ref prodvided.

        Args:
            project: Name of the Looker project to use.
            branch: Name of the branch to update.
            ref: The ref to update the branch from.
        """
        logger.debug(
            f"Updating branch '{branch}' on project '{project}' to ref '{ref}'"
        )

        body = {"name": branch, "ref": ref}
        url = utils.compose_url(self.api_url, path=["projects", project, "git_branch"])
        response = self.session.put(url=url, json=body, timeout=TIMEOUT_SEC)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-update-branch",
                title="Couldn't update Git branch.",
                status=response.status_code,
                detail=(
                    f"Unable to update branch '{branch}' "
                    f"in project '{project}' using ref '{ref}'. "
                    "Please try again."
                ),
                response=response,
            )

    def delete_branch(self, project: str, branch: str):
        """Deletes a branch in the given project.

        Args:
            project: Name of the Looker project to use.
            branch: Name of the branch to delete.
        """
        logger.debug(f"Deleting branch '{branch}' in project '{project}'")

        url = utils.compose_url(
            self.api_url, path=["projects", project, "git_branch", branch]
        )
        response = self.session.delete(url=url, timeout=TIMEOUT_SEC)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-delete-branch",
                title="Couldn't delete Git branch.",
                status=response.status_code,
                detail=(
                    f"Unable to delete branch '{branch}' "
                    f"in project '{project}'. Please try again."
                ),
                response=response,
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
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-get-data-tests",
                title="Couldn't retrieve all data tests.",
                status=response.status_code,
                detail=(
                    f"Unable to retrieve all data tests for "
                    f"project '{project}'. Please try again."
                ),
                response=response,
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
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-run-data-test",
                title="Couldn't run data test.",
                status=response.status_code,
                detail=(
                    f"Unable to run one or more data tests for "
                    f"project '{project}'. Please try again."
                ),
                response=response,
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
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-get-lookml",
                title="Couldn't retrieve models and explores.",
                status=response.status_code,
                detail="Unable to retrieve LookML details. Please try again.",
                response=response,
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
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-get-dimension-lookml",
                title="Couldn't retrieve dimensions.",
                status=response.status_code,
                detail=(
                    "Unable to retrieve dimension LookML details "
                    f"for explore '{model}/{explore}'. Please try again."
                ),
                response=response,
            )

        return response.json()["fields"]["dimensions"]

    @backoff.on_exception(backoff.expo, (Timeout,), max_tries=2)
    def create_query(self, model: str, explore: str, dimensions: List[str]) -> Dict:
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
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-create-query",
                title="Couldn't create query.",
                status=response.status_code,
                detail=(
                    f"Failed to create query for {model}/{explore}/"
                    f'{"*" if len(dimensions) > 1 else dimensions[0]}. '
                    "Please try again."
                ),
                response=response,
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
        return result

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

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-launch-query",
                title="Couldn't launch query.",
                status=response.status_code,
                detail=(
                    "Failed to create query task for "
                    f"query '{query_id}'. Please try again."
                ),
                response=response,
            )

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

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-get-query-results",
                title="Couldn't get results for the specified query tasks.",
                status=response.status_code,
                detail=(
                    "Failed to get the results for "
                    f"{len(query_task_ids)} query tasks. "
                    "Please try again."
                ),
                response=response,
            )

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
