import sys
import asyncio
from typing import Sequence, List, Dict, Any, Optional
import aiohttp
import requests
import fonz.utils as utils
from fonz.lookml import Project, Model, Explore, Dimension
from fonz.logger import GLOBAL_LOGGER as logger
from fonz.printer import print_start, print_pass, print_fail, print_error, print_stats
from fonz.exceptions import ConnectionError, ValidationError, FonzException


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

        self.base_url = url.rstrip("/")
        self.api_url = f"{self.base_url}:{port}/api/{api}/"
        self.client_id = client_id
        self.client_secret = client_secret
        self.branch = branch
        self.project = project
        self.session = requests.Session()
        self.lookml: Project = None
        self.messages: List[str] = []
        self.error_count = 0

        logger.debug(f"Instantiated Fonz object for url: {self.api_url}")

    def connect(self) -> None:
        """Authenticate, start a dev session, check out specified branch."""

        logger.info("Authenticating Looker credentials. \n")

        url = utils.compose_url(self.api_url, path=["login"])
        body = {"client_id": self.client_id, "client_secret": self.client_secret}
        response = self.session.post(url=url, data=body)
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
        url = utils.compose_url(self.api_url, path=["session"])
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
            self.api_url, path=["projects", self.project, "git_branch"]
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

    def build_project(self) -> Project:
        """Create a representation of the desired project's LookML."""

        models_json = self.get_models()
        models = []
        for model_json in models_json:
            model = Model.from_json(model_json)
            if model.project == self.project:
                for explore in model.explores:
                    dimensions_json = self.get_dimensions(model.name, explore.name)
                    for dimension_json in dimensions_json:
                        explore.add_dimension(Dimension.from_json(dimension_json))
                models.append(model)

        self.lookml = Project(self.project, models)

    def count_explores(self):
        """Return the total number of explores in the project."""

        explore_count = 0
        for model in self.lookml.models:
            explore_count += len(model.explores)
        return explore_count

    def validate(self, batch=False):
        explore_count = self.count_explores()
        index = 0
        for model in self.lookml.models:
            for explore in model.explores:
                index += 1
                print_start(explore.name, index, explore_count)

                self.validate_explore(model, explore, batch)

                if explore.errored:
                    print_fail(explore.name, index, explore_count)
                else:
                    print_pass(explore.name, index, explore_count)

    def report_results(self, batch: bool = False):
        """Displays the overall results of the completed validation."""

        explore_count = self.count_explores()
        for model in self.lookml.get_errored_models():
            for explore in model.get_errored_explores():
                if batch:
                    self.error_count += 1
                    message = (
                        f"Error in {model.name}/{explore.name}: "
                        f"{explore.error_message}\n\n"
                    )
                    print_error(message)
                else:
                    for dimension in explore.get_errored_dimensions():
                        self.error_count += 1
                        message = (
                            f"Error in {model.name}/{dimension.name}: "
                            f"{dimension.error_message}\n\n"
                            f"LookML in question is here: "
                            f"{self.base_url + dimension.url}"
                        )
                        print_error(message)

        print_stats(self.error_count, explore_count)
        if self.error_count > 0:
            raise ValidationError(
                f'Found {self.error_count} SQL errors in project "{self.project}"'
            )

    def get_models(self) -> List[JsonDict]:
        """Get all models and explores from the LookmlModel endpoint."""

        logger.debug("Getting all models and explores in Looker instance.")
        url = utils.compose_url(self.api_url, path=["lookml_models"])
        response = self.session.get(url=url)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise FonzException(
                f'Unable to retrieve explores.\nError raised: "{error}"'
            )

        return response.json()

    def get_dimensions(self, model: str, explore_name: str) -> List[str]:
        """Get dimensions for an explore from the LookmlModel endpoint."""

        logger.debug(f"Getting dimensions for {explore_name}")
        url = utils.compose_url(
            self.api_url, path=["lookml_models", model, "explores", explore_name]
        )
        response = self.session.get(url=url)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise FonzException(
                f'Unable to get dimensions for explore "{explore_name}".\n'
                f'Error raised: "{error}"'
            )

        return response.json()["fields"]["dimensions"]

    def create_query(self, model: str, explore_name: str, dimensions: List[str]) -> int:
        """Build a Looker query using all the specified dimensions."""

        logger.debug(f"Creating query for {explore_name}")
        url = utils.compose_url(self.api_url, path=["queries"])
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
        url = utils.compose_url(self.api_url, path=["queries", query_id, "run", "json"])
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
        url = utils.compose_url(self.api_url, path=["queries", query_id, "run", "sql"])
        response = self.session.get(url=url)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise FonzException(
                f'Failed to obtain SQL for query "{query_id}".\nError raised: "{error}"'
            )
        sql = response.text

        return sql

    async def query_explore(self, model: Model, explore: Explore):
        async with aiohttp.ClientSession(headers=self.session.headers) as async_session:
            dimensions = [dimension.name for dimension in explore.dimensions]
            query_id = self.create_query(model.name, explore.name, dimensions)
            explore.query_id = query_id
            result = self.run_query(query_id)

            if not result:
                return
            elif "looker_error" in result[0]:
                error_message = result[0]["looker_error"]
                for lookml_object in [explore, model]:
                    lookml_object.errored = True
                explore.error_message = error_message

    async def query_dimension(
        self, model: Model, explore: Explore, dimension: Dimension
    ):
        async with aiohttp.ClientSession(headers=self.session.headers) as async_session:
            query_id = self.create_query(model.name, explore.name, [dimension.name])
            dimension.query_id = query_id
            result = self.run_query(query_id)

            if not result:
                return
            elif "looker_error" in result[0]:
                error_message = result[0]["looker_error"]
                for lookml_object in [dimension, explore, model]:
                    lookml_object.errored = True
                dimension.error_message = error_message

    def validate_explore(
        self, model: Model, explore: Explore, batch: bool = False
    ) -> None:
        """Query selected dimensions in an explore and return any errors."""

        loop = asyncio.get_event_loop()
        tasks = []

        if batch:
            task = loop.create_task(self.query_explore(model, explore))
            tasks.append(task)
        else:
            for dimension in explore.dimensions:
                task = loop.create_task(self.query_dimension(model, explore, dimension))
                tasks.append(task)

        loop.run_until_complete(asyncio.gather(*tasks))
        # Wait for the underlying connections to close for graceful shutdown
        loop.run_until_complete(asyncio.sleep(0))
        loop.close()

    def handle_sql_error(
        self,
        model_name: str,
        explore_name: str,
        query_id: int,
        message: str,
        url: str = None,
    ):
        sql = self.get_query_sql(query_id)
        sql = sql.replace("\n\n", "\n")
        filename = f"./logs/{explore_name}.sql"
        with open(filename, "w+") as file:
            file.write(sql)

        message = self.add_error_context(message, sql, url)
        message = f"Error in {model_name}/{explore_name}: {message}"
        self.messages.append(message)
        logger.debug(message)

    def add_error_context(
        self, message: str, sql: str, url: str = None, show_sql: bool = True
    ) -> None:

        if show_sql:
            line_number = utils.parse_error_line_number(message)
            sql_context = utils.extract_sql_context(sql, line_number)
            message = message + "\n\n" + sql_context
        if url:
            message = (
                message + "\n\n" + f"LookML in question is here: {self.base_url + url}"
            )

        return message
