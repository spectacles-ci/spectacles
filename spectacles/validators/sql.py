from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Iterator, List, Optional, Tuple

import pydantic
from tabulate import tabulate

from spectacles.client import LookerClient
from spectacles.exceptions import SpectaclesException, SqlError
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.lookml import CompiledSql, Dimension, Explore
from spectacles.models import (
    CompletedQueryResult,
    ErrorQueryResult,
    InterruptedQueryResult,
    QueryResult,
)
from spectacles.printer import print_header
from spectacles.utils import consume_queue, halt_queue

QUERY_TASK_LIMIT = 250
DEFAULT_CHUNK_SIZE = 500
DEFAULT_QUERY_CONCURRENCY = 10
DEFAULT_RUNTIME_THRESHOLD = 5
EXPIRED_QUERY_WAIT_TIME = 300
EXPIRED_RETRY_LIMIT = 1
ProfilerTableRow = Tuple[str, str, float, str, str]


@dataclass
class Query:
    explore: Explore
    dimensions: tuple[Dimension, ...]
    query_id: str | None = None
    explore_url: str | None = None
    errored: bool | None = None
    runtime: float | None = None
    expired_at: float | None = None
    expired_retries: int = 0

    def __post_init__(self) -> None:
        # Confirm that all dimensions are from the Explore associated here
        if len(set((d.model_name, d.explore_name) for d in self.dimensions)) > 1:
            raise ValueError("All Dimensions must be from the same model and explore")
        elif self.dimensions[0].explore_name != self.explore.name:
            raise ValueError("Dimension.explore_name must equal Query.explore.name")
        elif self.dimensions[0].model_name != self.explore.model_name:
            raise ValueError("Dimension.model_name must equal Query.explore.model_name")

    def __repr__(self) -> str:
        return f"Query(explore={self.explore.name} n={len(self.dimensions)})"

    def divide(self) -> Iterator[Query]:
        if not self.errored:
            raise TypeError("Query.errored must be True to divide")
        if len(self.dimensions) < 2:
            raise ValueError("Query must have at least 2 dimensions to divide")

        midpoint = len(self.dimensions) // 2
        yield Query(self.explore, self.dimensions[:midpoint])
        yield Query(self.explore, self.dimensions[midpoint:])

    def to_profiler_format(self) -> ProfilerTableRow:
        if self.runtime is None:
            raise TypeError("Query has no runtime")
        if self.query_id is None:
            raise TypeError(
                "Query.query_id cannot be None, run Query.create to get a query ID"
            )
        if self.explore_url is None:
            raise TypeError(
                "Query.explore_url cannot be None, "
                "run Query.create to get an explore URL"
            )
        return (
            self.explore.name,
            self.dimensions[0].name if len(self.dimensions) == 1 else "*",
            self.runtime,
            self.query_id,
            self.explore_url,
        )


def print_profile_results(queries: List[Query], runtime_threshold: int) -> None:
    """Defined here instead of in .printer to avoid circular type imports."""
    HEADER_CHAR = "."
    print_header("Query profiler results", char=HEADER_CHAR, leading_newline=False)
    if queries:
        queries_by_runtime = sorted(
            queries,
            key=lambda x: x.runtime if x.runtime is not None else -1,
            reverse=True,
        )
        output = tabulate(
            [query.to_profiler_format() for query in queries_by_runtime],
            headers=[
                "Explore",
                "Dimension(s)",
                "Runtime (s)",
                "Query ID",
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
        concurrency: int = DEFAULT_QUERY_CONCURRENCY,
        runtime_threshold: int = DEFAULT_RUNTIME_THRESHOLD,
    ):
        self.client = client
        self.concurrency = concurrency
        self.runtime_threshold = runtime_threshold
        self._task_to_query: dict[str, Query] = {}
        self._long_running_queries: List[Query] = []

    async def compile_explore(self, explore: Explore) -> CompiledSql:
        if explore.skipped:
            sql = ""
        else:
            dimensions = [dimension.name for dimension in explore.dimensions]
            query_body = {
                "model": explore.model_name,
                "view": explore.name,
                "fields": dimensions,
                "limit": 0,
                "filter_expression": "1=2",
            }
            sql = await self.client.run_inline_query(
                query_body=query_body,
                result_format="sql",
                model=explore.model_name,
                explore=explore.name,
            )

        return CompiledSql.from_explore(explore, sql)

    async def compile_dimension(self, dimension: Dimension) -> CompiledSql:
        query_body = {
            "model": dimension.model_name,
            "view": dimension.explore_name,
            "fields": [dimension.name],
            "limit": 0,
            "filter_expression": "1=2",
        }
        sql = await self.client.run_inline_query(
            query_body=query_body,
            result_format="sql",
            model=dimension.model_name,
            explore=dimension.explore_name,
            dimension=dimension.name,
        )
        return CompiledSql.from_dimension(dimension, sql)

    async def search(
        self,
        explores: tuple[Explore, ...],
        fail_fast: bool,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        profile: bool = False,
        result_format: str = "json_bi",
    ) -> None:
        queries_to_run: asyncio.Queue[Optional[Query]] = asyncio.Queue()
        running_queries: asyncio.Queue[str] = asyncio.Queue()
        query_slot = asyncio.Semaphore(self.concurrency)

        workers = (
            asyncio.create_task(
                self._run_query(
                    queries_to_run,
                    running_queries,
                    query_slot,
                    result_format,
                ),
                name="run_query",
            ),
            asyncio.create_task(
                self._get_query_results(
                    queries_to_run, running_queries, fail_fast, query_slot
                ),
                name="get_query_results",
            ),
        )

        try:
            for explore in explores:
                # Sorting makes it more likely to prune the tree faster in binsearch
                dimensions = tuple(sorted(explore.dimensions))
                if explore.skipped:
                    continue
                elif len(dimensions) <= chunk_size:
                    queries_to_run.put_nowait(Query(explore, dimensions))
                else:
                    for i in range(0, len(dimensions), chunk_size):
                        chunk = dimensions[i : i + chunk_size]
                        query = Query(explore, chunk)
                        queries_to_run.put_nowait(query)

            # Wait for all work to complete
            await queries_to_run.join()
            await running_queries.join()
            logger.debug("Successfully joined all queues")
        except KeyboardInterrupt:
            logger.info(
                "\n\n" + "Please wait, asking Looker to cancel any running queries..."
            )
            task_ids = []
            while not running_queries.empty():
                task_id = running_queries.get_nowait()
                task_ids.append(task_id)
                await self.client.cancel_query_task(task_id)
            if task_ids:
                message = (
                    f"Attempted to cancel {len(task_ids)} running "
                    f"{'query' if len(task_ids) == 1 else 'queries'}."
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
        finally:
            # Shut down the workers gracefully
            for worker in workers:
                worker.cancel()
            results = await asyncio.gather(*workers, return_exceptions=True)
            for result in results:
                if isinstance(result, asyncio.CancelledError):
                    pass
                elif isinstance(result, Exception):
                    raise result

        if profile:
            print_profile_results(self._long_running_queries, self.runtime_threshold)

    async def _run_query(
        self,
        queries_to_run: asyncio.Queue[Optional[Query]],
        running_queries: asyncio.Queue[str],
        query_slot: asyncio.Semaphore,
        result_format: str = "json_bi",
    ) -> None:
        try:
            # End execution if a sentinel is received from the queue
            while (query := await queries_to_run.get()) is not None:
                logger.debug("Waiting to acquire a query slot")
                await query_slot.acquire()
                result = await self.client.create_query(
                    model=query.dimensions[0].model_name,
                    explore=query.dimensions[0].explore_name,
                    dimensions=[dimension.name for dimension in query.dimensions],
                    fields=["id", "share_url"],
                )
                query.query_id = result["id"]
                query.explore_url = result["share_url"]
                logger.debug(f"Running query {query!r} [qid={query.query_id}]")
                if query.query_id is None:
                    raise TypeError(
                        "Query.query_id cannot be None, "
                        "run Query.create to get a query ID"
                    )
                task_id = await self.client.create_query_task(
                    query.query_id, result_format
                )
                self._task_to_query[task_id] = query
                running_queries.put_nowait(task_id)

            logger.debug("Received sentinel, shutting down")

        except Exception:
            logger.error(
                "Encountered an exception while running a query:", exc_info=True
            )
            logger.debug("Waiting for the running_queries queue to clear")
            await running_queries.join()
            raise
        finally:
            # This only gets called if a sentinel is received or exception is raised.
            # We need to mark all remaining tasks as finished so Queue.join can unblock
            logger.debug("Marking all tasks in queries_to_run queue as done")
            halt_queue(queries_to_run)

    async def _get_query_results(
        self,
        queries_to_run: asyncio.Queue[Optional[Query]],
        running_queries: asyncio.Queue[str],
        fail_fast: bool,
        query_slot: asyncio.Semaphore,
    ) -> None:
        try:
            while True:
                task_ids = consume_queue(running_queries, limit=QUERY_TASK_LIMIT)
                if not task_ids:
                    logger.debug("No running queries, waiting for one to start...")
                    await asyncio.sleep(0.5)
                    continue

                raw = await self.client.get_query_task_multi_results(task_ids)
                for task_id, result in raw.items():
                    try:
                        query_result = QueryResult.model_validate(result).root
                    except pydantic.ValidationError as validation_error:
                        logger.debug(
                            f"Unable to parse unexpected Looker API response format: {result}"
                        )
                        raise SpectaclesException(
                            name="unexpected-query-result-format",
                            title="Encountered an unexpected query result format.",
                            detail=(
                                "Unable to extract error details from the Looker API's "
                                "response. The unexpected response has been logged."
                            ),
                        ) from validation_error
                    logger.debug(
                        f"Query task {task_id} status is: {query_result.status}"
                    )

                    # Append long-running queries for the profiler
                    if isinstance(
                        query_result, (CompletedQueryResult, ErrorQueryResult)
                    ):
                        query = self._task_to_query[task_id]
                        query.runtime = query_result.runtime
                        if (
                            query_result.runtime
                            and query_result.runtime > self.runtime_threshold
                        ):
                            self._long_running_queries.append(query)
                        if query_result.status == "complete":
                            query_slot.release()
                            query.errored = False
                            query.explore.queried = True
                            queries_to_run.task_done()
                        else:
                            query_slot.release()
                            query.errored = True

                            # Fail fast, assign the error(s) to its explore
                            if fail_fast:
                                explore = query.explore
                                explore.queried = True
                                for error in query_result.get_valid_errors():
                                    line_number = (
                                        error.sql_error_loc.line
                                        if error.sql_error_loc
                                        else None
                                    )
                                    explore.errors.append(
                                        SqlError(
                                            model=explore.model_name,
                                            explore=explore.name,
                                            dimension=None,
                                            sql=query_result.sql,
                                            message=error.full_message,
                                            line_number=line_number,
                                            explore_url=query.explore_url,
                                        )
                                    )

                            # Make child queries and put them back on the queue
                            elif len(query.dimensions) > 1:
                                for child in query.divide():
                                    await queries_to_run.put(child)

                            # Assign the error(s) to its dimension
                            elif len(query.dimensions) == 1:
                                dimension = query.dimensions[0]
                                dimension.queried = True
                                for error in query_result.get_valid_errors():
                                    line_number = (
                                        error.sql_error_loc.line
                                        if error.sql_error_loc
                                        else None
                                    )
                                    dimension.errors.append(
                                        SqlError(
                                            model=dimension.model_name,
                                            explore=dimension.explore_name,
                                            dimension=dimension.name,
                                            sql=query_result.sql,
                                            message=error.full_message,
                                            line_number=line_number,
                                            lookml_url=dimension.url,
                                            explore_url=query.explore_url,
                                        )
                                    )

                            else:
                                raise ValueError(
                                    "Query had an unexpected number of dimensions. "
                                    "Queries must have at least one dimension, but "
                                    f"{query!r} had {len(query.dimensions)} dimensions."
                                )

                            # Indicate there are no more queries or subqueries to run
                            queries_to_run.task_done()

                    elif (
                        isinstance(query_result, InterruptedQueryResult)
                        and query_result.status == "killed"
                    ):
                        query = self._task_to_query[task_id]
                        query.errored = True
                        explore = query.explore
                        explore.queried = True
                        explore.errors.append(
                            SqlError(
                                model=explore.model_name,
                                explore=explore.name,
                                dimension=None,
                                sql="",
                                message=(
                                    "Couldn't finish testing "
                                    f"{explore.model_name}.{explore.name} "
                                    "because the test query was killed "
                                    "in the database. "
                                ),
                                explore_url=query.explore_url,
                            )
                        )
                        query_slot.release()
                        queries_to_run.task_done()

                    elif (
                        isinstance(query_result, InterruptedQueryResult)
                        and query_result.status == "expired"
                    ):
                        query = self._task_to_query[task_id]
                        query.expired_at = query.expired_at or time.time()
                        expired_for = time.time() - query.expired_at
                        if expired_for > EXPIRED_QUERY_WAIT_TIME:
                            # Stop waiting for query, decide if we should retry
                            if query.expired_retries < EXPIRED_RETRY_LIMIT:
                                logger.debug(
                                    f"Query task {task_id} expired for "
                                    f"over {EXPIRED_QUERY_WAIT_TIME} seconds. "
                                    "Creating a new query task to try again."
                                )
                                query.expired_at = None
                                query.expired_retries += 1
                                await queries_to_run.put(query)
                            else:
                                logger.debug(
                                    f"Query task {task_id} keeps expiring, "
                                    f"even after {EXPIRED_RETRY_LIMIT + 1} tries. "
                                    "Giving up on it."
                                )
                                query.errored = True
                                explore = query.explore
                                explore.queried = True  # Not exactly, but close enough
                                explore.errors.append(
                                    SqlError(
                                        model=explore.model_name,
                                        explore=explore.name,
                                        dimension=None,
                                        sql="",
                                        message=(
                                            "Couldn't finish testing "
                                            f"{explore.model_name}.{explore.name} "
                                            "because queries repeatedly expired "
                                            "in Looker."
                                        ),
                                        explore_url=query.explore_url,
                                    )
                                )
                            query_slot.release()
                            queries_to_run.task_done()
                        else:
                            await running_queries.put(task_id)

                    else:
                        # Query still running, put the task back on the queue
                        await running_queries.put(task_id)

                # Notify queue that all task IDs were processed
                for _ in range(len(task_ids)):
                    running_queries.task_done()

                await asyncio.sleep(0.5)
        except Exception:
            logger.error(
                "Encountered an exception while retrieving results:", exc_info=True
            )
            # Put a sentinel on the run query queue to shut it down
            consume_queue(queries_to_run)
            queries_to_run.put_nowait(None)
            # Wait until the sentinel has been consumed and handled
            while not queries_to_run.empty():
                logger.debug("Waiting for the queries_to_run queue to clear")
                # _run_query can get bogged down waiting for query slots, so free them
                if query_slot.locked():
                    query_slot.release()
                await asyncio.sleep(1)
            raise
        finally:
            # This only gets called if an exception is raised.
            # We need to mark all remaining tasks as finished so Queue.join can unblock
            logger.debug("Marking all tasks in running_queries queue as done")
            halt_queue(running_queries)
