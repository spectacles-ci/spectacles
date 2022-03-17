from typing import List, Dict, Optional
import time
from dataclasses import dataclass
import backoff  # type: ignore
import requests
from requests.exceptions import Timeout, HTTPError, ConnectionError
import spectacles.utils as utils
from spectacles.types import JsonDict
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.exceptions import SpectaclesException, LookerApiError

TIMEOUT_SEC = 300
BACKOFF_EXCEPTIONS = (Timeout, HTTPError, ConnectionError)


@dataclass(frozen=True)  # Token is immutable
class AccessToken:
    access_token: str
    token_type: str
    expires_in: int
    expires_at: float

    def __str__(self) -> str:
        return self.access_token

    @property
    def expired(self) -> bool:
        return False if time.time() < self.expires_at else True


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
        port: Optional[int] = None,
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
        if port is None and self.base_url.endswith("cloud.looker.com"):
            # GCP-hosted instance, so use default port of 443 with HTTPS
            if not self.base_url.startswith("https"):
                raise SpectaclesException(
                    name="invalid-base-url",
                    title="Looker instance base URL is not valid.",
                    detail="The URL must be an HTTPS URL.",
                )
            self.api_url: str = f"{self.base_url}/api/{api_version}/"
        else:
            self.api_url = f"{self.base_url}:{port or 19999}/api/{api_version}/"
        self.client_id: str = client_id
        self.client_secret: str = client_secret
        self.api_version: float = api_version
        self.access_token: Optional[AccessToken] = None
        self.session: requests.Session = requests.Session()
        self.workspace: str = "production"

        self.authenticate()

    def authenticate(self) -> None:
        """Logs in to Looker's API using a client ID/secret pair and an API version.

        Args:
            client_id: Looker API client ID.
            client_secret: Looker API client secret.
            api_version: Desired API version to use for requests.

        """
        logger.debug("Authenticating Looker API credentials")

        url = utils.compose_url(self.api_url, path=["login"])
        body = {"client_id": self.client_id, "client_secret": self.client_secret}
        self.session.auth = NullAuth()
        # This should not use `self.post` or it will create a recursive loop
        response = self.session.post(url=url, data=body, timeout=TIMEOUT_SEC)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-authenticate",
                title="Couldn't authenticate to the Looker API.",
                status=response.status_code,
                detail=(
                    f"Unable to authenticate with client ID '{self.client_id}'. "
                    "Check that your credentials are correct and try again."
                ),
                response=response,
            )

        result = response.json()
        if "expires_at" not in result:
            # Calculate the expiration time with a one-minute buffer
            result["expires_at"] = time.time() + result["expires_in"] - 60
        self.access_token = AccessToken(**result)
        self.session.headers = {  # type: ignore
            "Authorization": f"token {self.access_token}"
        }

        looker_version = self.get_looker_release_version()
        logger.info(
            f"Connected to Looker version {looker_version} "
            f"using Looker API {self.api_version}"
        )

    @backoff.on_exception(
        backoff.expo,
        BACKOFF_EXCEPTIONS,
        max_tries=2,
    )
    def request(self, method: str, url: str, *args, **kwargs) -> requests.Response:
        if self.access_token and self.access_token.expired:
            logger.debug("Looker API access token has expired, requesting a new one")
            self.authenticate()
            if self.workspace == "dev":
                self.update_workspace("dev")
        return self.session.request(method, url, *args, **kwargs)

    def get(self, url, *args, **kwargs) -> requests.Response:
        return self.request("GET", url, *args, **kwargs)

    def post(self, url, *args, **kwargs) -> requests.Response:
        return self.request("POST", url, *args, **kwargs)

    def patch(self, url, *args, **kwargs) -> requests.Response:
        return self.request("PATCH", url, *args, **kwargs)

    def put(self, url, *args, **kwargs) -> requests.Response:
        return self.request("PUT", url, *args, **kwargs)

    def delete(self, url, *args, **kwargs) -> requests.Response:
        return self.request("DELETE", url, *args, **kwargs)

    def get_looker_release_version(self) -> str:
        """Gets the version number of connected Looker instance.

        Returns:
            str: Looker instance release version number (e.g. 6.22.12)

        """
        logger.debug("Checking Looker instance release version")

        url = utils.compose_url(self.api_url, path=["versions"])

        response = self.get(url=url, timeout=TIMEOUT_SEC)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-get-version",
                title="Couldn't get Looker's release version.",
                status=response.status_code,
                detail=(
                    "Unable to get the release version of your Looker instance. "
                    "Please try again."
                ),
                response=response,
            )

        return response.json()["looker_release_version"]

    def get_workspace(self) -> str:
        """Gets the session workspace.

        Args:
            project: Name of the Looker project to use.

        Returns:
            str: The session workspace, dev or production.
        """
        logger.debug("Getting the workspace in use by this session")
        url = utils.compose_url(self.api_url, path=["session"])
        response = self.get(url=url, timeout=TIMEOUT_SEC)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-get-workspace",
                title="Couldn't get the workspace.",
                status=response.status_code,
                detail=(
                    "Unable to get the workspace in use by this session. "
                    "Please try again."
                ),
                response=response,
            )
        return response.json()["workspace_id"]

    def update_workspace(self, workspace: str) -> None:
        """Updates the session workspace.

        Args:
            workspace: The workspace to switch to, either 'production' or 'dev'
        """
        logger.debug(f"Updating session to use the {workspace} workspace")
        url = utils.compose_url(self.api_url, path=["session"])
        body = {"workspace_id": workspace}
        response = self.patch(url=url, json=body, timeout=TIMEOUT_SEC)
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
        self.workspace = workspace

    def get_all_branches(self, project: str) -> List[str]:
        """Returns a list of git branches in the project repository.

        Args:
            project: Name of the Looker project to use.
        """
        logger.debug(f"Getting all Git branches in project '{project}'")
        url = utils.compose_url(
            self.api_url, path=["projects", project, "git_branches"]
        )
        response = self.get(url=url, timeout=TIMEOUT_SEC)
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
        logger.debug(f"Setting project '{project}' branch to '{branch}'")
        url = utils.compose_url(self.api_url, path=["projects", project, "git_branch"])
        body = {"name": branch}
        response = self.put(url=url, json=body, timeout=TIMEOUT_SEC)
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
        logger.debug("Resetting branch to remote.")
        url = utils.compose_url(
            self.api_url, path=["projects", project, "reset_to_remote"]
        )
        response = self.post(url=url, timeout=TIMEOUT_SEC)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-reset-remote",
                title="Couldn't checkout Git branch.",
                status=response.status_code,
                detail=(
                    "Unable to reset local Git branch "
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
        logger.debug("Getting manifest details")
        url = utils.compose_url(self.api_url, path=["projects", project, "manifest"])
        response = self.get(url=url, timeout=TIMEOUT_SEC)

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
        response = self.get(url=url, timeout=TIMEOUT_SEC)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-get-active-branch",
                title="Couldn't determine active Git branch.",
                status=response.status_code,
                detail=(
                    f"Unable to get active branch for project '{project}'. "
                    "Please check that the project exists and try again."
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

    def create_branch(self, project: str, branch: str, ref: Optional[str] = None):
        """Creates a branch in the given project.

        Args:
            project: Name of the Looker project to use.
            branch: Name of the branch to create.
            ref: The ref to create the branch from.
        """
        body = {"name": branch}
        message = f"Creating branch '{branch}' on project '{project}'"
        detail = (
            f"Unable to create branch '{branch}' "
            f"in project '{project}'. "
            "Confirm the branch doesn't already exist and try again."
        )

        if ref:
            body["ref"] = ref
            message += f" with ref '{ref}'"
            detail = (
                f"Unable to create branch '{branch}' "
                f"in project '{project}' using ref '{ref}'. "
                "Confirm the branch doesn't already exist and try again."
            )

        logger.debug(message)

        url = utils.compose_url(self.api_url, path=["projects", project, "git_branch"])
        response = self.post(url=url, json=body, timeout=TIMEOUT_SEC)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-create-branch",
                title="Couldn't create new Git branch.",
                status=response.status_code,
                detail=detail,
                response=response,
            )

    def hard_reset_branch(self, project: str, branch: str, ref: str):
        """Hard resets a branch to the ref prodvided.

        DANGER: hard reset will be force pushed to the remote. Unsaved changes and
            commits may be permanently lost.

        Args:
            project: Name of the Looker project to use.
            branch: Name of the branch to update.
            ref: The ref to update the branch from.
        """
        logger.debug(
            f"Hard resetting branch '{branch}' on project '{project}' to ref '{ref}'"
        )

        body = {"name": branch, "ref": ref}
        url = utils.compose_url(self.api_url, path=["projects", project, "git_branch"])
        response = self.put(url=url, json=body, timeout=TIMEOUT_SEC)

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
        response = self.delete(url=url, timeout=TIMEOUT_SEC)

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
        response = self.get(url=url, timeout=TIMEOUT_SEC)

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

    def run_lookml_test(
        self, project: str, model: str = None, test: str = None
    ) -> List[JsonDict]:
        """Runs all LookML/data tests for a given project and model (optional)

        This command only runs tests in production, as the Looker API doesn't currently
        allow us to run data tests on a specific branch.

        Args:
            project: Name of the Looker project to use
            model: Optional name of the LookML model to restrict testing to

        Returns:
            List[JsonDict]: JSON response containing any LookML/data test errors

        """
        if model is None and test is None:
            logger.debug(f"Running all LookML tests for project '{project}'")
        elif model is None and test is not None:
            logger.debug(f"Running LookML test '{test}'")
        elif model is not None and test is None:
            logger.debug(f"Running all LookML tests for model '{model}'")
        elif model is not None and test is not None:
            logger.debug(f"Running LookML test '{test}' in model '{model}'")

        url = utils.compose_url(
            self.api_url, path=["projects", project, "lookml_tests", "run"]
        )

        params = {}
        if model is not None:
            params["model"] = model
        if test is not None:
            params["test"] = test
        response = self.session.get(url=url, params=params, timeout=TIMEOUT_SEC)

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

    def get_lookml_models(self, fields: Optional[List] = None) -> List[JsonDict]:
        """Gets all models and explores from the LookmlModel endpoint.

        Returns:
            List[JsonDict]: JSON response containing LookML models and explores.

        """
        logger.debug(f"Getting all models and explores from {self.base_url}")
        if fields is None:
            fields = []

        params = {}
        if fields:
            params["fields"] = fields

        url = utils.compose_url(self.api_url, path=["lookml_models"], params=params)
        response = self.get(url=url, timeout=TIMEOUT_SEC)
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
        logger.debug(f"Getting all dimensions from explore {model}/{explore}")
        params = {"fields": ["fields"]}
        url = utils.compose_url(
            self.api_url,
            path=["lookml_models", model, "explores", explore],
            params=params,
        )
        response = self.get(url=url, timeout=TIMEOUT_SEC)
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

    def create_query(
        self, model: str, explore: str, dimensions: List[str], fields: List = None
    ) -> Dict:
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

        params: Dict[str, list] = {}
        if fields is None:
            params["fields"] = []
        else:
            params["fields"] = fields

        url = utils.compose_url(self.api_url, path=["queries"], params=params)
        response = self.post(url=url, json=body, timeout=TIMEOUT_SEC)
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
        params = {"fields": ["id"]}
        url = utils.compose_url(self.api_url, path=["query_tasks"], params=params)

        response = self.post(
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
        response = self.get(
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
        self.delete(url=url, timeout=TIMEOUT_SEC)

        # No raise_for_status() here because Looker API seems to give a 404
        # if you try to cancel a finished query which can happen as part of cleanup

    def content_validation(self) -> JsonDict:
        logger.debug("Validating all content in Looker")
        url = utils.compose_url(self.api_url, path=["content_validation"])
        response = self.get(
            url=url, timeout=3600
        )  # 1 hour timeout for content validation

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-validate-content",
                title="Couldn't validate Looks and Dashboards.",
                status=response.status_code,
                detail=("Failed to run the content validator. Please try again."),
                response=response,
            )

        result = response.json()
        return result

    def lookml_validation(self, project) -> JsonDict:
        logger.debug(f"Validating LookML for project '{project}'")
        url = utils.compose_url(self.api_url, path=["projects", project, "validate"])
        response = self.post(url=url, timeout=TIMEOUT_SEC)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-validate-lookml",
                title=f"Couldn't validate LookML in project {project}.",
                status=response.status_code,
                detail=("Failed to run the LookML validator. Please try again."),
                response=response,
            )

        result = response.json()
        return result

    def all_folders(self) -> List[JsonDict]:
        logger.debug("Getting information about all folders")
        url = utils.compose_url(self.api_url, path=["folders"])
        response = self.get(url=url, timeout=TIMEOUT_SEC)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-get-folders",
                title="Couldn't obtain project folders.",
                status=response.status_code,
                detail=("Failed to get all folders."),
                response=response,
            )

        result = response.json()
        return result

    @backoff.on_exception(backoff.expo, (Timeout,), max_tries=2)
    def run_query(self, query_id: int) -> str:
        """Returns the compiled SQL for a given query ID.

        The corresponding Looker API endpoint allows us to run queries with a variety
        of result formats, however we only use the `sql` result format, which doesn't
        run the query but does return its compiled SQL.

        If a Timeout exception is received, attempts to retry.

        """
        # Using old-style string formatting so that strings are formatted lazily
        logger.debug("Retrieving the SQL for query ID %s", query_id)

        url = utils.compose_url(self.api_url, path=["queries", query_id, "run", "sql"])
        response = self.get(url=url, timeout=TIMEOUT_SEC)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise LookerApiError(
                name="unable-to-retrieve-compiled-sql",
                title="Couldn't retrieve compiled SQL.",
                status=response.status_code,
                detail=(
                    f"Failed to retrieve compiled SQL for query '{query_id}'. "
                    "Please try again."
                ),
                response=response,
            )

        result = response.text

        return result


class NullAuth(requests.auth.AuthBase):
    """A custom auth class which ensures requests does not override authorization
    headers with netrc file credentials if present.
    """

    def __call__(self, r):
        return r
