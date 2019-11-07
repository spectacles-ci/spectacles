from typing import List, Sequence, DefaultDict, Tuple
import asyncio
import time
from abc import ABC, abstractmethod
from collections import defaultdict
import aiohttp
from spectacles.client import LookerClient
from spectacles.lookml import Project, Model, Explore, Dimension
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.exceptions import SqlError, DataTestError, SpectaclesException
import spectacles.printer as printer


class Validator(ABC):  # pragma: no cover
    """Defines abstract base interface for validators.

    Not intended to be used directly, only inherited.

    Attributes:
        client: Looker API client.

    """

    def __init__(self, client: LookerClient):
        self.client = client

    @abstractmethod
    def validate(self):
        raise NotImplementedError


class DataTestValidator(Validator):
    """Runs LookML/data tests for a given project.

    Args:
        client: Looker API client.
        project: Name of the LookML project to validate.

    """

    def __init__(self, client: LookerClient, project: str):
        super().__init__(client)
        self.project = project

    def validate(self) -> List[DataTestError]:
        tests = self.client.all_lookml_tests(self.project)
        test_count = len(tests)
        printer.print_header(
            f"Running {test_count} {'test' if test_count == 1 else 'tests'}"
        )
        errors = []
        test_results = self.client.run_lookml_test(self.project)
        for result in test_results:
            if not result["success"]:
                for error in result["errors"]:
                    errors.append(
                        DataTestError(
                            path=f"{result['model_name']}/{result['test_name']}",
                            message=error["message"],
                        )
                    )
        return errors


class SqlValidator(Validator):
    """Runs and validates the SQL for each selected LookML dimension.

    Args:
        client: Looker API client.
        project: Name of the LookML project to validate.

    Attributes:
        project: LookML project object representation.
        query_tasks: Mapping of query task IDs to LookML objects

    """

    timeout = aiohttp.ClientTimeout(total=300)
    MIN_LOOKER_VERSION = "6.22.12"

    def __init__(self, client: LookerClient, project: str):
        super().__init__(client)
        meets_required_version = self.client.validate_looker_release_version(
            required_version=self.MIN_LOOKER_VERSION
        )
        if not meets_required_version:
            raise SpectaclesException(
                "SQL validation requires version "
                f"{self.MIN_LOOKER_VERSION} of Looker or higher."
            )
        self.project = Project(project, models=[])
        self.query_tasks: dict = {}

    @staticmethod
    def parse_selectors(selectors: List[str]) -> DefaultDict[str, set]:
        """Parses explore selectors with the format 'model_name.explore_name'.

        Args:
            selectors: List of selector strings in 'model_name.explore_name' format.
                The '*' wildcard selects all models or explores. For instance,
                'model_name.*' would select all explores in the 'model_name' model.

        Returns:
            DefaultDict[str, set]: A hierarchy of selected model names (keys) and
                explore names (values).

        """
        selection: DefaultDict = defaultdict(set)
        for selector in selectors:
            try:
                model, explore = selector.split(".")
            except ValueError:
                raise SpectaclesException(
                    f"Explore selector '{selector}' is not valid.\n"
                    "Instead, use the format 'model_name.explore_name'. "
                    f"Use 'model_name.*' to select all explores in a model."
                )
            else:
                selection[model].add(explore)
        return selection

    # TODO: Refactor this so it's more obvious how selection works
    def _select(self, choices: Sequence[str], select_from: Sequence) -> Sequence:
        unique_choices = set(choices)
        select_from_names = set(each.name for each in select_from)
        difference = unique_choices.difference(select_from_names)
        if difference:
            raise SpectaclesException(
                f"{select_from[0].__class__.__name__}"
                f'{"" if len(difference) == 1 else "s"} '
                + ", ".join(difference)
                + f" not found in LookML under project '{self.project.name}'"
            )
        return [each for each in select_from if each.name in unique_choices]

    def build_project(self, selectors: List[str]) -> None:
        """Creates an object representation of the project's LookML.

        Args:
            selectors: List of selector strings in 'model_name.explore_name' format.
                The '*' wildcard selects all models or explores. For instance,
                'model_name.*' would select all explores in the 'model_name' model.

        """
        selection = self.parse_selectors(selectors)
        logger.info(
            f"Building LookML project hierarchy for project {self.project.name}"
        )

        all_models = [
            Model.from_json(model) for model in self.client.get_lookml_models()
        ]
        project_models = [
            model for model in all_models if model.project == self.project.name
        ]

        # Expand wildcard operator to include all specified or discovered models
        selected_model_names = selection.keys()
        if "*" in selected_model_names:
            explore_names = selection.pop("*")
            for model in project_models:
                selection[model.name].update(explore_names)

        selected_models = self._select(
            choices=tuple(selection.keys()), select_from=project_models
        )

        for model in selected_models:
            # Expand wildcard operator to include all specified or discovered explores
            selected_explore_names = selection[model.name]
            if "*" in selected_explore_names:
                selected_explore_names.remove("*")
                selected_explore_names.update(
                    set(explore.name for explore in model.explores)
                )

            selected_explores = self._select(
                choices=tuple(selected_explore_names), select_from=model.explores
            )

            for explore in selected_explores:
                dimensions_json = self.client.get_lookml_dimensions(
                    model.name, explore.name
                )
                for dimension_json in dimensions_json:
                    dimension = Dimension.from_json(dimension_json)
                    dimension.url = self.client.base_url + dimension.url
                    if not dimension.ignore:
                        explore.add_dimension(dimension)

            model.explores = selected_explores

        self.project.models = selected_models

    def validate(self, mode: str = "batch") -> List[SqlError]:
        """Queries selected explores and returns any errors.

        Args:
            batch: When true, runs one query per explore (using all dimensions). When
                false, runs one query per dimension. Batch mode increases query speed
                but can only return the first error encountered for each dimension.

        Returns:
            List[SqlError]: SqlErrors encountered while querying the explore.

        """
        explore_count = self._count_explores()
        printer.print_header(
            f"Testing {explore_count} "
            f"{'explore' if explore_count == 1 else 'explores'} "
            f"[{mode} mode]"
        )

        errors = self._query(mode)
        if mode == "hybrid" and self.project.errored:
            errors = self._query(mode)

        for model in sorted(self.project.models, key=lambda x: x.name):
            for explore in sorted(model.explores, key=lambda x: x.name):
                if explore.errored:
                    logger.info(
                        f"✗ {printer.red(model.name + '.' + explore.name)} failed"
                    )
                else:
                    logger.info(
                        f"✓ {printer.green(model.name + '.' + explore.name)} passed"
                    )

        return errors

    def _query(self, mode: str = "batch") -> List[SqlError]:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        session = aiohttp.ClientSession(
            loop=loop, headers=self.client.session.headers, timeout=self.timeout
        )
        tasks = []
        for model in self.project.models:
            for explore in model.explores:
                if mode == "batch" or (mode == "hybrid" and not explore.queried):
                    logger.debug("Querying one explore at at time")
                    task = loop.create_task(
                        self._query_explore(session, model, explore)
                    )
                    tasks.append(task)
                elif mode == "single" or (mode == "hybrid" and explore.errored):
                    logger.debug("Querying one dimension at at time")
                    for dimension in explore.dimensions:
                        task = loop.create_task(
                            self._query_dimension(session, model, explore, dimension)
                        )
                        tasks.append(task)

        query_task_ids = list(loop.run_until_complete(asyncio.gather(*tasks)))
        loop.run_until_complete(session.close())
        loop.run_until_complete(asyncio.sleep(0.250))
        loop.close()

        MAX_QUERY_FETCH = 250

        tasks_to_check = query_task_ids[:MAX_QUERY_FETCH]
        del query_task_ids[:MAX_QUERY_FETCH]
        logger.debug(f"{len(query_task_ids)} left in queue")
        tasks_to_check, errors = self._get_query_results(tasks_to_check)

        while tasks_to_check or query_task_ids:
            number_of_tasks_to_add = MAX_QUERY_FETCH - len(tasks_to_check)
            tasks_to_check.extend(query_task_ids[:number_of_tasks_to_add])
            del query_task_ids[:number_of_tasks_to_add]
            logger.debug(f"{len(query_task_ids)} left in queue")
            tasks_to_check, more_errors = self._get_query_results(tasks_to_check)
            errors.extend(more_errors)
            if tasks_to_check or query_task_ids:
                time.sleep(0.5)

        return errors

    @staticmethod
    def _extract_error_details(query_result: dict) -> dict:
        data = query_result["data"]
        if isinstance(data, dict):
            errors = data.get("errors") or [data.get("error")]
            first_error = errors[0]
            message = first_error["message_details"]
            if not isinstance(message, str):
                raise TypeError(
                    "Unexpected message type. Expected a str, "
                    f"received type {type(message)}: {message}"
                )
            sql = data["sql"]
            if first_error.get("sql_error_loc"):
                line_number = first_error["sql_error_loc"]["line"]
            else:
                line_number = None
        elif isinstance(data, list):
            message = data[0]
            line_number = None
            sql = None
        else:
            raise TypeError(
                "Unexpected error response type. "
                "Expected a dict or a list, "
                f"received type {type(data)}: {data}"
            )

        return {"message": message, "sql": sql, "line_number": line_number}

    def _get_query_results(
        self, query_task_ids: List[str]
    ) -> Tuple[List[str], List[SqlError]]:
        results = self.client.get_query_task_multi_results(query_task_ids)
        still_running = []
        errors = []

        for query_task_id, query_result in results.items():
            query_status = query_result["status"]
            logger.debug("Query task %s status is %s", query_task_id, query_status)

            if query_status in ("running", "added", "expired"):
                still_running.append(query_task_id)
                continue
            elif query_status in ("complete", "error"):
                lookml_object = self.query_tasks[query_task_id]
                lookml_object.queried = True
            else:
                raise SpectaclesException(
                    f'Unexpected query result status "{query_status}" '
                    "returned by the Looker API"
                )

            if query_status == "error":
                try:
                    details = self._extract_error_details(query_result)
                except (KeyError, TypeError, IndexError) as error:
                    raise SpectaclesException(
                        "Encountered an unexpected API query result format, "
                        "unable to extract error details. "
                        f"The query result was: {query_result}"
                    ) from error
                sql_error = SqlError(
                    path=lookml_object.name,
                    url=getattr(lookml_object, "url", None),
                    **details,
                )
                lookml_object.error = sql_error
                errors.append(sql_error)

        return still_running, errors

    async def _query_explore(
        self, session: aiohttp.ClientSession, model: Model, explore: Explore
    ) -> str:
        """Creates and executes a query with a single explore.

        Args:
            model: Object representation of LookML model.
            explore: Object representation of LookML explore.

        Returns:
            str: Query task ID for the running query.

        """
        dimensions = [dimension.name for dimension in explore.dimensions]
        query_id = await self.client.create_query(
            session, model.name, explore.name, dimensions
        )
        query_task_id = await self.client.create_query_task(session, query_id)

        self.query_tasks[query_task_id] = explore
        return query_task_id

    async def _query_dimension(
        self,
        session: aiohttp.ClientSession,
        model: Model,
        explore: Explore,
        dimension: Dimension,
    ) -> str:
        """Creates and executes a query with a single dimension.

        Args:
            model: Object representation of LookML model.
            explore: Object representation of LookML explore.
            dimension: Object representation of LookML dimension.

        Returns:
            str: Query task ID for the running query.

        """
        query_id = await self.client.create_query(
            session, model.name, explore.name, [dimension.name]
        )
        query_task_id = await self.client.create_query_task(session, query_id)

        self.query_tasks[query_task_id] = dimension
        return query_task_id

    def _count_explores(self) -> int:
        """Counts the explores in the LookML project hierarchy.

        Returns:
            int: The number of explores in the LookML project.

        """
        explore_count = 0
        for model in self.project.models:
            explore_count += len(model.explores)
        return explore_count
