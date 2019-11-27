from typing import List, Sequence, DefaultDict
import asyncio
from abc import ABC, abstractmethod
from collections import defaultdict
import aiohttp
from spectacles.client import LookerClient
from spectacles.lookml import Project, Model, Explore, Dimension
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.exceptions import SqlError, DataTestError, SpectaclesException
import spectacles.printer as printer
import signal


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
            message = f"{result['model_name']}.{result['test_name']}"
            if result["success"]:
                printer.print_validation_result("success", message)
            else:
                for error in result["errors"]:
                    printer.print_validation_result("error", message)
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

    def __init__(self, client: LookerClient, project: str, concurrency: int = 10):
        super().__init__(client)

        self.project = Project(project, models=[])
        self.query_tasks: dict = {}
        self.query_slots = asyncio.BoundedSemaphore(concurrency)
        self.running_query_tasks: asyncio.Queue = asyncio.Queue()

    @staticmethod
    def parse_selectors(selectors: List[str]) -> DefaultDict[str, set]:
        """Parses explore selectors with the format 'model_name/explore_name'.

        Args:
            selectors: List of selector strings in 'model_name/explore_name' format.
                The '*' wildcard selects all models or explores. For instance,
                'model_name/*' would select all explores in the 'model_name' model.

        Returns:
            DefaultDict[str, set]: A hierarchy of selected model names (keys) and
                explore names (values).

        """
        selection: DefaultDict = defaultdict(set)
        for selector in selectors:
            try:
                model, explore = selector.split("/")
            except ValueError:
                raise SpectaclesException(
                    f"Explore selector '{selector}' is not valid.\n"
                    "Instead, use the format 'model_name/explore_name'. "
                    f"Use 'model_name/*' to select all explores in a model."
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
            selectors: List of selector strings in 'model_name/explore_name' format.
                The '*' wildcard selects all models or explores. For instance,
                'model_name/*' would select all explores in the 'model_name' model.

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

        loop = asyncio.get_event_loop()

        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for s in signals:
            loop.add_signal_handler(
                s, lambda s=s: asyncio.create_task(self.shutdown(s, loop))
            )

        errors = list(loop.run_until_complete(self._query(mode)))
        if mode == "hybrid" and self.project.errored:
            errors = list(loop.run_until_complete(self._query(mode)))

        for model in sorted(self.project.models, key=lambda x: x.name):
            for explore in sorted(model.explores, key=lambda x: x.name):
                message = f"{model.name}.{explore.name}"
                if explore.errored:
                    printer.print_validation_result("error", message)
                else:
                    printer.print_validation_result("success", message)

        return errors

    async def shutdown(self, signal, loop):
        logger.info("\n\n" + "Please wait, asking Looker to cancel any running queries")
        logger.debug("Cleaning up async tasks.")
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        # Nothing executes beyond this point because of CancelledErrors

    async def _query(self, mode: str = "batch") -> List[SqlError]:
        session = aiohttp.ClientSession(
            headers=self.client.session.headers, timeout=self.timeout
        )

        query_tasks = []
        for model in self.project.models:
            for explore in model.explores:
                if mode == "batch" or (mode == "hybrid" and not explore.queried):
                    task = asyncio.create_task(
                        self._query_explore(session, model, explore)
                    )
                    query_tasks.append(task)
                elif mode == "single" or (mode == "hybrid" and explore.errored):
                    for dimension in explore.dimensions:
                        task = asyncio.create_task(
                            self._query_dimension(session, model, explore, dimension)
                        )
                        query_tasks.append(task)

        queries = asyncio.gather(*query_tasks)
        query_results = asyncio.create_task(
            self._check_for_results(session, query_tasks)
        )
        try:
            results = await asyncio.gather(queries, query_results)
        except asyncio.CancelledError:
            query_task_ids = []
            while not self.running_query_tasks.empty():
                query_task_ids.append(await self.running_query_tasks.get())
            cancel_query_tasks = []
            for query_task_id in query_task_ids:
                task = asyncio.create_task(
                    self.client.cancel_query_task(session, query_task_id)
                )
                cancel_query_tasks.append(task)

            await asyncio.gather(*cancel_query_tasks)

            message = "Spectacles was manually interrupted. "
            if query_task_ids:
                message += (
                    "Spectacles attempted to cancel "
                    f"{len(query_task_ids)} running "
                    f"{'query' if len(query_task_ids) == 1 else 'queries'}."
                )
            else:
                message += "No queries were running at the time."
            raise SpectaclesException(message)
        else:
            errors = results[1]  # Ignore the results from creating the queries
            return errors
        finally:
            await session.close()

    @staticmethod
    def _extract_error_details(query_result: dict) -> dict:
        data = query_result["data"]
        if isinstance(data, dict):
            errors = data.get("errors") or [data.get("error")]
            first_error = errors[0]
            message = " ".join(
                filter(
                    None,
                    [first_error.get("message"), first_error.get("message_details")],
                )
            )
            sql = data.get("sql")
            error_loc = first_error.get("sql_error_loc")
            if error_loc:
                line_number = error_loc.get("line")
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

    async def _run_query(
        self,
        session: aiohttp.ClientSession,
        model: str,
        explore: str,
        dimensions: List[str],
    ) -> str:
        query_id = await self.client.create_query(session, model, explore, dimensions)
        await self.query_slots.acquire()  # Wait for available slots before launching
        query_task_id = await self.client.create_query_task(session, query_id)
        await self.running_query_tasks.put(query_task_id)
        return query_task_id

    async def _get_query_results(
        self, session: aiohttp.ClientSession
    ) -> List[SqlError]:
        logger.debug("%d queries running", self.running_query_tasks.qsize())
        try:
            # Empty the queue (up to 250) to get all running query tasks
            query_task_ids: List[str] = []
            while not self.running_query_tasks.empty() and len(query_task_ids) <= 250:
                query_task_ids.append(await self.running_query_tasks.get())

            logger.debug("Getting results for %d query tasks", len(query_task_ids))
            results = await self.client.get_query_task_multi_results(
                session, query_task_ids
            )
            pending_task_ids = []
            errors = []

            for query_task_id, query_result in results.items():
                query_status = query_result["status"]
                logger.debug("Query task %s status is %s", query_task_id, query_status)
                if query_status in ("running", "added", "expired"):
                    pending_task_ids.append(query_task_id)
                    # Put the running query tasks back in the queue
                    await self.running_query_tasks.put(query_task_id)
                    query_task_ids.remove(query_task_id)
                    continue
                elif query_status in ("complete", "error"):
                    query_task_ids.remove(query_task_id)
                    # We can release a query slot for each completed query
                    self.query_slots.release()
                    lookml_object = self.query_tasks[query_task_id]
                    lookml_object.queried = True

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
                else:
                    raise SpectaclesException(
                        f'Unexpected query result status "{query_status}" '
                        "returned by the Looker API"
                    )
        except asyncio.CancelledError:
            logger.debug(
                "Cancelled result fetching, putting "
                f"{self.running_query_tasks.qsize()} query task IDs back in the queue"
            )
            for query_task_id in query_task_ids:
                await self.running_query_tasks.put(query_task_id)
            logger.debug("Restored query task IDs to queue")
            raise

        return errors

    async def _check_for_results(
        self, session: aiohttp.ClientSession, query_tasks: List[asyncio.Task]
    ):
        results = []
        while (
            any(not task.done() for task in query_tasks)
            or not self.running_query_tasks.empty()
        ):
            if not self.running_query_tasks.empty():
                result = await self._get_query_results(session)
                results.extend(result)
            await asyncio.sleep(0.5)

        return results

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
        query_task_id = await self._run_query(
            session, model.name, explore.name, dimensions
        )
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
        query_task_id = await self._run_query(
            session, model.name, explore.name, [dimension.name]
        )
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
