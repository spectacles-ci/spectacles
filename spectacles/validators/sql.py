from typing import Union, Dict, Any, List, Optional
import time
from spectacles.validators.validator import Validator
from spectacles.client import LookerClient
from spectacles.lookml import Dimension, Explore
from spectacles.types import QueryMode
from spectacles.exceptions import SpectaclesException, SqlError
from spectacles.logger import GLOBAL_LOGGER as logger


class Query:
    """Stores IDs and a reference to the LookML object being queried"""

    def __init__(
        self,
        query_id: str,
        lookml_ref: Union[Dimension, Explore],
        explore_url: str,
        query_task_id: Optional[str] = None,
    ):
        self.query_id = query_id
        self.lookml_ref = lookml_ref
        self.explore_url = explore_url
        self.query_task_id = query_task_id


class QueryResult:
    """Stores ID, query status, and error details for a completed query task"""

    def __init__(
        self, query_task_id: str, status: str, error: Optional[Dict[str, Any]] = None
    ):
        self.query_task_id = query_task_id
        self.status = status
        self.error = error


class SqlValidator(Validator):
    """Runs and validates the SQL for each selected LookML dimension.

    Args:
        client: Looker API client.
        project: Name of the LookML project to validate.

    Attributes:
        project: LookML project object representation.
        query_tasks: Mapping of query task IDs to LookML objects

    """

    def __init__(self, client: LookerClient, project: str, concurrency: int = 10):
        super().__init__(client, project)
        self.query_slots = concurrency
        self._running_queries: List[Query] = []
        # Lookup used to retrieve the LookML object
        self._query_by_task_id: Dict[str, Query] = {}

    def get_query_by_task_id(self, query_task_id: str) -> Query:
        return self._query_by_task_id[query_task_id]

    def get_running_query_tasks(self) -> List[str]:
        return [
            query.query_task_id
            for query in self._running_queries
            if query.query_task_id
        ]

    def build_project(
        self,
        selectors: Optional[List[str]] = None,
        exclusions: Optional[List[str]] = None,
        build_dimensions: bool = True,
    ) -> None:
        super().build_project(selectors, exclusions, build_dimensions)

    def validate(self, mode: QueryMode = "batch") -> Dict[str, Any]:
        """Queries selected explores and returns the project tree with errors."""
        self._query_by_task_id = {}

        self._create_and_run(mode)
        if mode == "hybrid" and self.project.errored:
            self._create_and_run(mode)

        return self.project.get_results(validator="sql", mode=mode)

    def _create_and_run(self, mode: QueryMode = "batch") -> None:
        """Runs a single validation using a specified mode"""
        queries: List[Query] = []
        try:
            queries = self._create_queries(mode)
            self._run_queries(queries)
        except KeyboardInterrupt:
            logger.info(
                "\n\n" + "Please wait, asking Looker to cancel any running queries..."
            )
            query_tasks = self.get_running_query_tasks()
            self._cancel_queries(query_tasks)
            if query_tasks:
                message = (
                    f"Attempted to cancel {len(query_tasks)} running "
                    f"{'query' if len(query_tasks) == 1 else 'queries'}."
                )
            else:
                message = (
                    "No queries were running at the time so nothing was cancelled."
                )
            raise SpectaclesException(
                name="validation-keyboard-interrupt",
                title="SQL validation was manually interrupted.",
                detail=message,
            )

    def _create_queries(self, mode: QueryMode) -> List[Query]:
        """Creates a list of queries to be executed for validation"""
        queries: List[Query] = []
        for model in self.project.models:
            for explore in model.explores:
                if mode == "batch" or (mode == "hybrid" and not explore.queried):
                    query = self._create_explore_query(explore, model.name)
                    queries.append(query)
                elif mode == "single" or (mode == "hybrid" and explore.errored):
                    explore_queries = self._create_dimension_queries(
                        explore, model.name
                    )
                    queries.extend(explore_queries)
        return queries

    def _create_explore_query(self, explore: Explore, model_name: str) -> Query:
        """Creates a single query with all dimensions of an explore"""
        dimensions = [dimension.name for dimension in explore.dimensions]
        query = self.client.create_query(model_name, explore.name, dimensions)
        return Query(query["id"], lookml_ref=explore, explore_url=query["share_url"])

    def _create_dimension_queries(
        self, explore: Explore, model_name: str
    ) -> List[Query]:
        """Creates individual queries for each dimension in an explore"""
        queries = []
        for dimension in explore.dimensions:
            query = self.client.create_query(model_name, explore.name, [dimension.name])
            query = Query(
                query["id"], lookml_ref=dimension, explore_url=query["share_url"]
            )
            queries.append(query)
        return queries

    def _run_queries(self, queries: List[Query]) -> None:
        """Creates and runs queries with a maximum concurrency defined by query slots"""
        QUERY_TASK_LIMIT = 250

        while queries or self._running_queries:
            if queries:
                logger.debug(f"Starting a new loop, {len(queries)} queries queued")
                self._fill_query_slots(queries)
            query_tasks = self.get_running_query_tasks()[:QUERY_TASK_LIMIT]
            logger.debug(f"Checking for results of {len(query_tasks)} query tasks")
            for query_result in self._get_query_results(query_tasks):
                self._handle_query_result(query_result)
            time.sleep(0.5)

    def _fill_query_slots(self, queries: List[Query]) -> None:
        """Creates query tasks until all slots are used or all queries are running"""
        while queries and self.query_slots > 0:
            logger.debug(
                f"{self.query_slots} available query slots, creating query task"
            )
            query = queries.pop(0)
            query_task_id = self.client.create_query_task(query.query_id)
            self.query_slots -= 1
            query.query_task_id = query_task_id
            self._query_by_task_id[query_task_id] = query
            self._running_queries.append(query)

    def _get_query_results(self, query_task_ids: List[str]) -> List[QueryResult]:
        """Returns ID, status, and error message for all query tasks"""
        query_results = []
        results = self.client.get_query_task_multi_results(query_task_ids)
        for query_task_id, result in results.items():
            status = result["status"]
            if status not in ("complete", "error", "running", "added", "expired"):
                raise SpectaclesException(
                    name="unexpected-query-result-status",
                    title="Encountered an unexpected query result status.",
                    detail=(
                        f"Query result status '{status}' was returned "
                        "by the Looker API."
                    ),
                )
            logger.debug(f"Query task {query_task_id} status is: {status}")
            query_result = QueryResult(query_task_id, status)
            if status == "error":
                try:
                    error_details = self._extract_error_details(result)
                except (KeyError, TypeError, IndexError) as error:
                    logger.debug(
                        f"Exiting because of unexpected query result format: {result}"
                    )
                    raise SpectaclesException(
                        name="unexpected-query-result-format",
                        title="Encountered an unexpected query result format.",
                        detail="Unable to extract error details. The unexpected result has been logged.",
                    ) from error
                else:
                    query_result.error = error_details
            query_results.append(query_result)
        return query_results

    def _handle_query_result(self, result: QueryResult) -> Optional[SqlError]:
        query = self.get_query_by_task_id(result.query_task_id)
        if result.status in ("complete", "error"):
            self._running_queries.remove(query)
            self.query_slots += 1
            lookml_object = query.lookml_ref
            lookml_object.queried = True

            if result.status == "error" and result.error:
                model_name = lookml_object.model_name
                dimension_name: Optional[str] = None
                if isinstance(lookml_object, Dimension):
                    explore_name = lookml_object.explore_name
                    dimension_name = lookml_object.name
                else:
                    explore_name = lookml_object.name

                sql_error = SqlError(
                    model=model_name,
                    explore=explore_name,
                    dimension=dimension_name,
                    explore_url=query.explore_url,
                    lookml_url=getattr(lookml_object, "url", None),
                    **result.error,
                )
                lookml_object.errors.append(sql_error)
                return sql_error
        return None

    @staticmethod
    def _extract_error_details(query_result: Dict) -> Optional[Dict]:
        """Extracts the relevant error fields from a Looker API response"""
        data = query_result["data"]
        if isinstance(data, dict):
            errors = data.get("errors") or [data.get("error")]
            try:
                first_error = next(
                    error
                    for error in errors
                    if error.get("message")
                    != "Note: This query contains derived tables with conditional SQL for Development Mode. "
                    "Query results in Production Mode might be different."
                )
            except StopIteration:
                return None
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

    def _cancel_queries(self, query_task_ids: List[str]) -> None:
        """Asks the Looker API to cancel specified queries"""
        for query_task_id in query_task_ids:
            self.client.cancel_query_task(query_task_id)
