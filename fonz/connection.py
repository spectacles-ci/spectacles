from typing import Sequence, List, Dict, Any, Optional
import fonz.utils as utils
from fonz.logger import GLOBAL_LOGGER as logger
from fonz.exceptions import SqlError, ConnectionError, FonzException
import requests
import sys

JsonDict = Dict[str, Any]


class Fonz:
    def __init__(
        self,
        url: str,
        client_id: str,
        client_secret: str,
        port: int,
        api: str,
        project: str = None,
        branch: str = None,
    ):
        """Instantiate Fonz and save authentication details and branch."""

        if not client_id:
            raise FonzException(
                "No Looker API client ID provided. "
                "Instructions for supplying credentials at "
                "https://github.com/dbanalyticsco/Fonz/blob/master/README.md"
            )
        if not client_secret:
            raise FonzException(
                "No Looker API client secret provided. "
                "Instructions for supplying credentials at "
                "https://github.com/dbanalyticsco/Fonz/blob/master/README.md"
            )

        self.base_url = f'{url.rstrip("/")}:{port}/api/{api}/'
        self.client_id = client_id
        self.client_secret = client_secret
        self.branch = branch
        self.project = project
        self.session = requests.Session()
        self.messages: List[str] = []

        logger.debug(f"Instantiated Fonz object for url: {self.base_url}")

    def connect(self) -> None:
        """Authenticate, start a dev session, check out specified branch."""

        logger.info("Authenticating Looker credentials. \n")

        url = utils.compose_url(self.base_url, path=["login"])
        body = {"client_id": self.client_id, "client_secret": self.client_secret}
        response = self.session.post(url=url, json=body)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise ConnectionError(
                f"Failed to authenticate to {url}\n"
                f'Attempted authentication with client ID "{self.client_id}"\n'
                f'Error raised: "{error}"'
            )

        access_token = response.json()["access_token"]
        self.session.headers = {"Authorization": f"token {access_token}"}

    def update_session(self) -> None:
        """Switch to a dev mode session and checkout the desired branch."""

        if not self.project:
            raise FonzException(
                "No Looker project name provided. "
                "Please include the desired project name with --project"
            )
        if not self.branch:
            raise FonzException(
                "No git branch provided. "
                "Please include the desired git branch name with --branch"
            )

        logger.debug("Updating session to use development workspace.")
        url = utils.compose_url(self.base_url, path=["session"])
        body = {"workspace_id": "dev"}
        response = self.session.patch(url=url, json=body)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise ConnectionError(
                f"Unable to update session to development workspace.\n"
                f'Error raised: "{error}"'
            )

        logger.debug(f"Setting git branch to: {self.branch}")
        url = utils.compose_url(
            self.base_url, path=["projects", self.project, "git_branch"]
        )
        body = {"name": self.branch}
        response = self.session.put(url=url, json=body)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise ConnectionError(
                f'Unable to set git branch to "{self.branch}".\n'
                f'Error raised: "{error}"'
            )

    def get_explores(self) -> List[JsonDict]:
        """Get all explores from the LookmlModel endpoint."""

        logger.debug("Getting all explores in Looker instance.")
        url = utils.compose_url(self.base_url, path=["lookml_models"])
        response = self.session.get(url=url)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise FonzException(
                f'Unable to retrieve explores.\nError raised: "{error}"'
            )

        explores = []

        logger.debug(f"Filtering explores for project: {self.project}")

        for model in response.json():
            if model["project_name"] == self.project:
                for explore in model["explores"]:
                    explores.append(
                        {"model": model["name"], "explore": explore["name"]}
                    )

        return explores

    def get_dimensions(self, model: str, explore_name: str) -> List[str]:
        """Get dimensions for an explore from the LookmlModel endpoint."""

        logger.debug(f"Getting dimensions for {explore_name}")
        url = utils.compose_url(
            self.base_url, path=["lookml_models", model, "explores", explore_name]
        )
        response = self.session.get(url=url)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise FonzException(
                f'Unable to get dimensions for explore "{explore_name}".\n'
                f'Error raised: "{error}"'
            )

        dimensions = []

        for dimension in response.json()["fields"]["dimensions"]:
            if "fonz: ignore" not in dimension["sql"]:
                dimensions.append(dimension["name"])

        return dimensions

    def create_query(self, model: str, explore_name: str, dimensions: List[str]) -> int:
        """Build a Looker query using all the specified dimensions."""

        logger.debug(f"Creating query for {explore_name}")
        url = utils.compose_url(self.base_url, path=["queries"])
        body = {"model": model, "view": explore_name, "fields": dimensions, "limit": 1}
        response = self.session.post(url=url, json=body)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise FonzException(
                f'Unable to create a query for "{model}/{explore_name}".\n'
                f'Error raised: "{error}"'
            )
        query_id = response.json()["id"]

        return query_id

    def run_query(self, query_id: int) -> List[JsonDict]:
        """Run a Looker query by ID and return the JSON result."""

        logger.debug(f"Running query {query_id}")
        url = utils.compose_url(
            self.base_url, path=["queries", query_id, "run", "json"]
        )
        response = self.session.get(url=url)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise FonzException(
                f'Failed to run query "{query_id}".\nError raised: "{error}"'
            )
        query_result = response.json()

        return query_result

    def get_query_sql(self, query_id: int) -> str:
        """Collect the SQL string for a Looker query."""

        logger.debug(f"Getting SQL for query {query_id}")
        url = utils.compose_url(self.base_url, path=["queries", query_id, "run", "sql"])
        response = self.session.get(url=url)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise FonzException(
                f'Failed to obtain SQL for query "{query_id}".\nError raised: "{error}"'
            )
        sql = response.text

        return sql

    def validate_explore(
        self, model: str, explore_name: str, dimensions: List[str]
    ) -> None:
        """Query selected dimensions in an explore and return any errors."""

        query_id = self.create_query(model, explore_name, dimensions)
        result = self.run_query(query_id)
        logger.debug(result)
        if not result:
            return
        elif "looker_error" in result[0]:
            error_message = result[0]["looker_error"]
            raise SqlError(query_id, explore_name, error_message)
        else:
            return

    def handle_sql_error(
        self, query_id: int, message: str, explore_name: str, show_sql: bool = True
    ) -> None:
        """Log and save SQL snippet and error message for later."""

        line_number = utils.parse_error_line_number(message)
        sql = self.get_query_sql(query_id)
        sql = sql.replace("\n\n", "\n")
        filename = f"./logs/{explore_name}.sql"
        with open(filename, "w+") as file:
            file.write(sql)
        full_message = f"Error in explore {explore_name}: {message}"
        if show_sql:
            sql_context = utils.extract_sql_context(sql, line_number)
            full_message = full_message + "\n\n" + sql_context
        self.messages.append(full_message)
        logger.debug(full_message)

    def validate_content(self) -> JsonDict:
        """Validate all content and return any JSON errors."""
        pass
