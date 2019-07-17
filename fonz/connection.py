import sys
import asyncio
from pathlib import Path
from typing import Sequence, List, Dict, Any, Optional
import aiohttp
import requests
import backoff
import fonz.utils as utils
from fonz.lookml import Project, Model, Explore, Dimension
from fonz.logger import GLOBAL_LOGGER as logger
from fonz.printer import print_start, print_pass, print_fail, print_error, print_stats
from fonz.exceptions import (
    ConnectionError,
    ValidationError,
    FonzException,
    QueryNotFinished,
    SqlError,
)


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
        if api not in ["3.0", "3.1"]:
            raise FonzException(
                f"API version {api} is not supported. Please a valid API version."
            )

        self.base_url = url.rstrip("/")
        self.api_url = f"{self.base_url}:{port}/api/{api}/"
        self.api_version = api
        self.client_id = client_id
        self.client_secret = client_secret
        self.branch = branch
        self.project = project
        self.session = requests.Session()
        self.lookml = Project(project, models=[])
        self.error_count = 0

        logger.debug(f"Instantiated Fonz object for url: {self.api_url}")

    def connect(self) -> None:
        """Authenticate, start a dev session, check out specified branch."""

        logger.info("Authenticating Looker credentials...")

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

        logger.info(
            f"Connection test completed successfully for {self.base_url}, "
            f"API version {self.api_version}\n"
        )

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

    def build_project(self):
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

        self.lookml.models = models

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
                    sql = explore.error.sql
                    line_number = explore.error.line_number

                    path = Path.cwd() / "logs" / model.name / f"{explore.name}.sql"
                    path.parent.mkdir(parents=True, exist_ok=True)
                    with path.open("w+") as file:
                        file.write(sql)

                    sql_context = utils.extract_sql_context(sql, line_number)
                    message = (
                        f"Error in {model.name}/{explore.name}: "
                        f"{explore.error.message}\n\n"
                        f"{sql_context}\n\n"
                        f"Full SQL logged to {path}"
                    )
                    print_error(message)
                else:
                    for dimension in explore.get_errored_dimensions():
                        self.error_count += 1
                        sql = dimension.error.sql
                        line_number = dimension.error.line_number

                        path = (
                            Path.cwd()
                            / "logs"
                            / model.name
                            / explore.name
                            / f"{dimension.name}.sql"
                        )
                        path.parent.mkdir(parents=True, exist_ok=True)
                        with path.open("w+") as file:
                            file.write(sql)

                        sql_context = utils.extract_sql_context(sql, line_number)
                        message = (
                            f"Error in {model.name}/{dimension.name}: "
                            f"{dimension.error.message}\n\n"
                            f"{sql_context}\n\n"
                            f"Full SQL logged to {path}\n"
                            f"LookML in question is here: "
                            f"{self.base_url + dimension.url}"
                        )
                        print_error(message)

        print_stats(self.error_count, explore_count)
        if self.error_count > 0:
            raise ValidationError(
                f"Found {self.error_count} SQL "
                f'{"errors" if self.error_count > 1 else "error"} '
                f'in project "{self.project}"'
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

    async def create_query(
        self, session, model: str, explore_name: str, dimensions: List[str]
    ) -> int:
        """Build a Looker query using all the specified dimensions."""

        logger.debug(
            f"Creating query for {model}/{explore_name} "
            f"with {len(dimensions)} dimensions"
        )
        body = {"model": model, "view": explore_name, "fields": dimensions, "limit": 1}
        url = utils.compose_url(self.api_url, path=["queries"])
        async with session.post(url=url, json=body) as response:
            result = await response.json()
        query_id = result["id"]
        return query_id

    async def run_query(self, session: aiohttp.ClientSession, query_id: int) -> str:
        """Run a Looker query asynchronously by ID and return the query task ID."""

        logger.debug(f"Starting query {query_id}")
        body = {"query_id": query_id, "result_format": "json"}
        url = utils.compose_url(self.api_url, path=["query_tasks"])
        async with session.post(url=url, json=body) as response:
            result = await response.json()
        query_task_id = result["id"]
        return query_task_id

    @backoff.on_exception(backoff.expo, aiohttp.ClientError, max_tries=2)
    @backoff.on_exception(backoff.expo, QueryNotFinished, max_value=1)
    async def get_query_results(
        self, session: aiohttp.ClientSession, query_task_id: str, explore_name: str
    ) -> List[JsonDict]:
        """Check for async query task results until they're ready."""

        logger.debug(f"Attempting to get results for query {query_task_id}")
        url = utils.compose_url(
            self.api_url, path=["query_tasks", query_task_id, "results"]
        )
        async with session.get(url=url) as response:
            if response.status == 204:
                logger.debug(f"Query task {query_task_id} not finished yet.")
                raise QueryNotFinished
            logger.debug(f"Received results from query {query_task_id}")
            result = await response.json()
            return result

    async def query_explore(self, model: Model, explore: Explore):
        async with aiohttp.ClientSession(
            headers=self.session.headers, raise_for_status=True
        ) as async_session:
            dimensions = [dimension.name for dimension in explore.dimensions]
            query_id = await self.create_query(
                async_session, model.name, explore.name, dimensions
            )
            explore.query_id = query_id
            query_task_id = await self.run_query(async_session, query_id)
            result = await self.get_query_results(
                async_session, query_task_id, explore.name
            )

            if not result:
                return
            elif isinstance(result, dict) and result.get("errors"):
                first_error = result["errors"][0]
                error_message = first_error["message_details"]
                line_number = first_error["sql_error_loc"]["line"]
                sql = result["sql"]

                explore.errored = True
                model.errored = True
                explore.error = SqlError(error_message, sql, line_number)

    async def query_dimension(
        self, model: Model, explore: Explore, dimension: Dimension
    ):
        async with aiohttp.ClientSession(
            headers=self.session.headers, raise_for_status=True
        ) as async_session:
            query_id = await self.create_query(
                async_session, model.name, explore.name, [dimension.name]
            )
            dimension.query_id = query_id
            query_task_id = await self.run_query(async_session, query_id)
            result = await self.get_query_results(
                async_session, query_task_id, explore.name
            )

            if not result:
                return
            elif isinstance(result, dict) and result.get("errors"):
                first_error = result["errors"][0]
                error_message = first_error["message_details"]
                line_number = first_error["sql_error_loc"]["line"]
                sql = result["sql"]

                dimension.errored = True
                explore.errored = True
                model.errored = True
                dimension.error = SqlError(error_message, sql, line_number)

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
