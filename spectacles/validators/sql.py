from dataclasses import dataclass
from tabulate import tabulate
from typing import Union, Dict, Any, List, Optional, Tuple
import itertools
import time
from spectacles.utils import chunks
from spectacles.client import LookerClient
from spectacles.lookml import Dimension, Explore, Project
from spectacles.exceptions import SpectaclesException, SqlError
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.printer import print_header

DEFAULT_CHUNK_SIZE = 500
ProfilerTableRow = Tuple[str, str, float, int, str]


@dataclass
class Query:
    query_id: int
    explore_url: str
    query_task_id: Optional[str] = None


@dataclass
class QueryResult:
    """Stores ID, query status, and error details for a completed query task"""

    query_task_id: str
    status: str
    runtime: Optional[float] = None
    error: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class ProfilerResult:
    """Stores the data needed to display results for the query profiler."""

    lookml_obj: Union[Dimension, Explore]
    runtime: float
    query: Query

    def format(self) -> ProfilerTableRow:
        """Return data in a format suitable for tabulate to print."""
        return (
            self.lookml_obj.__class__.__name__.lower(),
            self.lookml_obj.name,
            self.runtime,
            self.query.query_id,
            self.query.explore_url,
        )


@dataclass
class SqlTest:
    queries: List[Query]
    lookml_ref: Union[Dimension, Explore]
    explore_url: str
    sql: Optional[str] = None
    query_task_id: Optional[str] = None
    status: Optional[str] = None
    runtime: Optional[float] = None
    error: Optional[SqlError] = None

    @property
    def failed(self) -> bool:
        return bool(self.error)

    @property
    def lookml_url(self) -> Optional[str]:
        return getattr(self.lookml_ref, "url", None)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, self.__class__):
            if self.sql and other.sql:
                return self.sql == other.sql and self.lookml_ref == other.lookml_ref
            else:
                return self.lookml_ref == other.lookml_ref
        else:
            return False

    def __hash__(self) -> int:
        if self.sql is None:
            raise ValueError("Test has no SQL defined")
        return hash(self.lookml_ref.model_name + self.lookml_ref.name + self.sql)

    def __dict__(self):
        metadata = {"explore_url": self.explore_url}
        if self.lookml_url:
            metadata["lookml_url"] = self.lookml_url
        output = {
            "lookml_type": self.lookml_ref.__class__.__name__,
            "passed": not self.failed,
            "metadata": metadata,
        }
        if self.error:
            output["errors"] = [self.error.__dict__]
        return output

    def get_query_by_task_id(self, query_task_id: str) -> Query:
        for query in self.queries:
            if query.query_task_id == query_task_id:
                return query
        raise KeyError(f"Query with query_task_id '{query_task_id}' not found in test")


def print_profile_results(
    results: List[ProfilerResult], runtime_threshold: int
) -> None:
    """Defined here instead of in .printer to avoid circular type imports."""
    HEADER_CHAR = "."
    print_header("Query profiler results", char=HEADER_CHAR, leading_newline=False)
    if results:
        results_by_runtime = sorted(
            results,
            key=lambda x: x.runtime if x.runtime is not None else -1,
            reverse=True,
        )
        output = tabulate(
            [result.format() for result in results_by_runtime],
            headers=[
                "Type",
                "Name",
                "Runtime (s)",
                "Query IDs",
                "Explore From Here",
            ],
            tablefmt="github",
            numalign="left",
            floatfmt=".1f",
        )
    else:
        output = f"All queries completed in less than {runtime_threshold} " "seconds."
    logger.info(output)
    print_header(HEADER_CHAR, char=HEADER_CHAR)


class SqlValidator:
    """Runs and validates the SQL for each selected LookML dimension.

    Args:
        client: Looker API client.
        project: Name of the LookML project to validate.
        concurrency: The number of simultaneous queries to run.
        runtime_threshold: When profiling, only display queries lasting longer
            than this.

    Attributes:
        project: LookML project object representation.
        query_tasks: Mapping of query task IDs to LookML objects

    """

    def __init__(
        self,
        client: LookerClient,
        concurrency: int = 10,
        runtime_threshold: int = 5,
    ):
        self.client = client
        self.query_slots = concurrency
        self.runtime_threshold = runtime_threshold
        # Lookup used to retrieve the LookML object
        self._test_by_task_id: Dict[str, SqlTest] = {}
        self._preemptive_cancellations: List[Query] = []
        self._long_running_tests: List[ProfilerResult] = []

    def create_tests(
        self,
        project: Project,
        compile_sql: bool = False,
        at_dimension_level: bool = False,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> List[SqlTest]:
        tests: List[SqlTest] = []
        if at_dimension_level:
            for explore in project.iter_explores():
                if not explore.skipped and explore.errored is not False:
                    for dimension in explore.dimensions:
                        test = self._create_dimension_test(dimension, compile_sql)
                        tests.append(test)
        else:
            for explore in project.iter_explores():
                test = self._create_explore_test(explore, compile_sql, chunk_size)
                tests.append(test)
        return tests

    def _create_explore_test(
        self,
        explore: Explore,
        compile_sql: bool = False,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> SqlTest:
        """Creates a SqlTest to query all dimensions in an explore"""
        if not explore.dimensions:
            raise AttributeError(
                "Explore object is missing dimensions, "
                "meaning this query won't have fields and will error. "
                "Often this happens because you didn't include dimensions "
                "when you built the project."
            )
        dimensions = [dimension.name for dimension in explore.dimensions]
        # Create a query that includes all dimensions
        main_query = self.client.create_query(
            explore.model_name, explore.name, dimensions, fields=["id", "share_url"]
        )
        sql = self.client.run_query(main_query["id"]) if compile_sql else None

        execution_queries: List[Query] = []
        if len(dimensions) > chunk_size:
            # Create separate chunked queries for execution, we don't store compiled SQL
            # or the Explore URL for these queries
            for chunk in chunks(dimensions, size=chunk_size):
                chunk_query = self.client.create_query(
                    explore.model_name, explore.name, chunk, fields=["id", "share_url"]
                )
                execution_queries.append(
                    Query(chunk_query["id"], chunk_query["share_url"])
                )
        else:
            execution_queries = [Query(main_query["id"], main_query["share_url"])]

        test = SqlTest(
            queries=execution_queries,
            lookml_ref=explore,
            explore_url=main_query["share_url"],
            sql=sql,
        )
        return test

    def _create_dimension_test(
        self, dimension: Dimension, compile_sql: bool = False
    ) -> SqlTest:
        query = self.client.create_query(
            dimension.model_name,
            dimension.explore_name,
            [dimension.name],
            fields=["id", "share_url"],
        )
        sql = self.client.run_query(query["id"]) if compile_sql else None
        test = SqlTest(
            queries=[Query(query["id"], query["share_url"])],
            lookml_ref=dimension,
            explore_url=query["share_url"],
            sql=sql,
        )
        return test

    def run_tests(self, tests: List[SqlTest], profile: bool = False):
        try:
            self._run_tests(tests)
        except KeyboardInterrupt:
            logger.info(
                "\n\n" + "Please wait, asking Looker to cancel any running queries..."
            )
            query_tasks = list(self._test_by_task_id.keys())
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

        if profile:
            print_profile_results(self._long_running_tests, self.runtime_threshold)

    def _run_tests(self, tests: List[SqlTest], fail_fast: bool = True) -> None:
        """Creates and runs tests with a maximum concurrency defined by query slots"""
        QUERY_TASK_LIMIT = 250
        test_by_query_id: Dict[int, SqlTest] = {
            query.query_id: test for test in tests for query in test.queries
        }

        def fill_query_slots(queries: List[Query]) -> None:
            """Creates query tasks until slots are full or all queries are running"""
            while queries and self.query_slots > 0:
                logger.debug(
                    f"{self.query_slots} available query slots, creating query task"
                )
                query = queries.pop(0)
                if query in self._preemptive_cancellations:
                    continue
                query_task_id = self.client.create_query_task(query.query_id)
                self.query_slots -= 1
                query.query_task_id = query_task_id
                # At query creation, we mapped tests by query ID, now we map to task ID
                self._test_by_task_id[query_task_id] = test_by_query_id[query.query_id]

        queries: List[Query] = list(
            itertools.chain.from_iterable(test.queries for test in tests)
        )
        while queries or self._test_by_task_id:
            if queries:
                logger.debug(f"Starting a new loop, {len(tests)} tests queued")
                fill_query_slots(queries)
            query_tasks = list(self._test_by_task_id.keys())[:QUERY_TASK_LIMIT]
            logger.debug(f"Checking for results of {len(query_tasks)} query tasks")
            for query_result in self._get_query_results(query_tasks):
                if query_result.status in ("complete", "error"):
                    self._handle_query_result(query_result, fail_fast)
            time.sleep(0.5)

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

            try:
                runtime: Optional[float] = float(result["data"]["runtime"])
            except KeyError:
                runtime = None

            query_result = QueryResult(query_task_id, status, runtime)
            if status == "error":
                try:
                    error_details = self._extract_error_details(result)
                except Exception as error:
                    logger.debug(
                        f"Unable to parse unexpected query result format: {result}"
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

    def _handle_query_result(self, result: QueryResult, fail_fast: bool = True) -> None:
        test = self._test_by_task_id.pop(result.query_task_id)
        self.query_slots += 1
        test.status = result.status
        test.runtime = (test.runtime or 0.0) + (result.runtime or 0.0)
        lookml_object = test.lookml_ref
        lookml_object.queried = True

        if result.runtime and result.runtime >= self.runtime_threshold:
            query: Query = test.get_query_by_task_id(result.query_task_id)
            self._long_running_tests.append(
                ProfilerResult(lookml_object, result.runtime, query)
            )

        if result.status == "error" and result.error:
            if fail_fast:
                # Once a test has an error, stop all other queries
                for query in test.queries:
                    self._preemptive_cancellations.append(query)

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
                lookml_url=test.lookml_url,
                explore_url=test.explore_url,
                **result.error,
            )
            test.error = sql_error
            lookml_object.errors.append(sql_error)

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
                    not in [
                        (
                            "Note: This query contains derived tables with conditional SQL for Development Mode. "
                            "Query results in Production Mode might be different."
                        ),
                        (
                            "Note: This query contains derived tables with Development Mode filters. "
                            "Query results in Production Mode might be different."
                        ),
                    ]
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
