from collections import defaultdict
import sys
import asyncio
from pathlib import Path
from typing import Sequence, Set, Collection, List, DefaultDict, Dict, Any, Optional
import aiohttp
import requests
import backoff  # type: ignore
import fonz.utils as utils
from fonz.lookml import Project, Model, Explore, Dimension
from fonz.logger import GLOBAL_LOGGER as logger
import fonz.printer as printer
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
        self, url: str, client_id: str, client_secret: str, port: int, api: float
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

        supported_api_versions = [3.0, 3.1]
        if api not in supported_api_versions:
            raise FonzException(
                f"API version {printer.color(str(api), 'bold')} is not supported. "
                "Please use one of these supported versions instead: "
                f"{', '.join(str(ver) for ver in sorted(supported_api_versions))}"
            )

        self.base_url = url.rstrip("/")
        self.api_url = f"{self.base_url}:{port}/api/{api}/"
        self.api_version = api
        self.client_id = client_id
        self.client_secret = client_secret
        self.session = requests.Session()
        self.lookml: Optional[Project] = None
        self.project: Optional[str] = None
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
            f"Connected to {printer.color(self.base_url, 'bold')} "
            f"using API version {printer.color(self.api_version, 'bold')}"
        )

    def update_session(self, project: str, branch: str) -> None:
        """Switch to a dev mode session and checkout the desired branch."""
        self.project = project

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

        logger.debug(f"Setting git branch to {printer.color(branch, 'bold')}")
        url = utils.compose_url(
            self.api_url, path=["projects", self.project, "git_branch"]
        )
        body = {"name": branch}
        response = self.session.put(url=url, json=body)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise ConnectionError(
                f"Unable to set git branch to {printer.color(branch, 'bold')}.\n"
                f'Error raised: "{error}"'
            )

        logger.info(f"Checked out branch {printer.color(branch, 'bold')}")

    def select(self, to_select: Collection[str], discovered: Sequence) -> Sequence:
        to_select = set(to_select)
        discovered_names = set(each.name for each in discovered)
        difference = to_select.difference(discovered_names)
        if difference:
            raise FonzException(
                f"{discovered[0].__class__.__name__}"
                f'{"" if len(difference) == 1 else "s"} '
                + ", ".join(printer.color(diff, "bold") for diff in difference)
                + " not found in LookML for project "
                f"{printer.color(self.project, 'bold')}."
            )
        return [each for each in discovered if each.name in to_select]

    @staticmethod
    def parse_selectors(selectors: List) -> DefaultDict[str, set]:
        selection: DefaultDict = defaultdict(set)

        for selector in selectors:
            try:
                model, explore = selector.split(".")
            except ValueError:
                raise FonzException(
                    f"Explore selector {printer.color(selector, 'bold')} is not valid.\n"
                    "Instead, use the format "
                    f"{printer.color('model_name.explore_name', 'bold')}. "
                    f"Use {printer.color('model_name.*', 'bold')} "
                    f"to select all explores in a model."
                )
            else:
                selection[model].add(explore)

        return selection

    def build_project(self, selectors: List[str]) -> None:
        """Create a representation of the desired project's LookML."""

        selection = self.parse_selectors(selectors)
        self.lookml = Project(self.project, models=[])

        logger.info(
            f"Building LookML hierarchy for {printer.color(self.project, 'bold')}..."
        )

        models_json = self.get_models()
        all_models = [Model.from_json(model_json) for model_json in models_json]
        project_models = [m for m in all_models if m.project == self.project]

        # Expand wildcard operator to include all specified or discovered models
        selected_model_names = selection.keys()
        if "*" in selected_model_names:
            explore_names = selection.pop("*")
            for model in project_models:
                selection[model.name].update(explore_names)

        selected_models = self.select(
            to_select=selection.keys(), discovered=project_models
        )

        for model in selected_models:
            # Expand wildcard operator to include all specified or discovered explores
            selected_explore_names = selection[model.name]
            if "*" in selected_explore_names:
                selected_explore_names.remove("*")
                selected_explore_names.update(
                    set(explore.name for explore in model.explores)
                )

            selected_explores = self.select(
                to_select=selected_explore_names, discovered=model.explores
            )

            for explore in selected_explores:
                dimensions_json = self.get_dimensions(model.name, explore.name)
                for dimension_json in dimensions_json:
                    dimension = Dimension.from_json(dimension_json)
                    if not dimension.ignore:
                        explore.add_dimension(dimension)

            model.explores = selected_explores

        self.lookml.models = selected_models

    def count_explores(self):
        """Return the total number of explores in the project."""

        explore_count = 0
        for model in self.lookml.models:
            explore_count += len(model.explores)
        return explore_count

    def validate(self, batch: bool = False) -> None:
        explore_count = self.count_explores()
        printer.print_header(
            f"Begin testing {explore_count} "
            f"{'explore' if explore_count == 1 else 'explores'}"
        )
        index = 0

        if not self.lookml:
            raise ValueError("No LookML model has been defined yet.")

        for model in self.lookml.models:
            for explore in model.explores:
                index += 1
                printer.print_start(explore.name, index, explore_count)

                self.validate_explore(model, explore, batch)

                if explore.errored:
                    printer.print_fail(explore.name, index, explore_count)
                else:
                    printer.print_pass(explore.name, index, explore_count)

    def report_results(self, batch: bool = False) -> None:
        """Displays the overall results of the completed validation."""
        printer.print_header("End testing session")
        explore_count = self.count_explores()

        if not self.lookml:
            raise ValueError("No LookML model has been defined yet.")

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

                    printer.print_sql_error(
                        f"{model.name}/{explore.name}",
                        explore.error.message,
                        sql,
                        line_number,
                        f"Full SQL logged to {path}",
                    )
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
                            / f"{dimension.name.split('.')[-1]}.sql"
                        )
                        path.parent.mkdir(parents=True, exist_ok=True)
                        with path.open("w+") as file:
                            file.write(sql)

                        printer.print_sql_error(
                            f"{model.name}/{dimension.name}",
                            dimension.error.message,
                            sql,
                            line_number,
                            f"Full SQL logged to {path}",
                            "LookML causing the error: "
                            f"{printer.color(self.base_url + dimension.url, 'cyan')}",
                        )

        exit_message = (
            f"Found {self.error_count} SQL "
            f'{"error" if self.error_count == 1 else "errors"} '
            f"in {self.project}"
        )
        printer.print_header(exit_message)
        if self.error_count > 0:
            raise ValidationError(exit_message)

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

    @backoff.on_exception(
        backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=2
    )
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

    @backoff.on_exception(
        backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=2
    )
    async def run_query(self, session: aiohttp.ClientSession, query_id: int) -> str:
        """Run a Looker query asynchronously by ID and return the query task ID."""

        logger.debug(f"Starting query {query_id}")
        body = {"query_id": query_id, "result_format": "json"}
        url = utils.compose_url(self.api_url, path=["query_tasks"])
        async with session.post(url=url, json=body) as response:
            result = await response.json()
        query_task_id = result["id"]
        return query_task_id

    @backoff.on_exception(
        backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=2
    )
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
        timeout = aiohttp.ClientTimeout(total=300)
        async with aiohttp.ClientSession(
            headers=self.session.headers, raise_for_status=True, timeout=timeout
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
                # Subtract one to account for the comment Looker appends to queries
                line_number = first_error["sql_error_loc"]["line"] - 1
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
                if first_error.get("sql_error_loc"):
                    line_number = first_error["sql_error_loc"]["line"]
                else:
                    line_number = None
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
