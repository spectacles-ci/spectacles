import asyncio
from typing import Optional
from unittest.mock import Mock, patch
import json
import logging
import pytest
import httpx
import respx
from spectacles.validators.sql import Query, SqlValidator
from spectacles.lookml import Explore, Dimension
from spectacles.exceptions import LookerApiError
from spectacles.client import LookerClient


@pytest.fixture
def validator(looker_client: LookerClient) -> SqlValidator:
    # TODO: Make sure we're mocking the login calls on client instantiation
    return SqlValidator(looker_client)


@pytest.fixture
def queries_to_run() -> "asyncio.Queue[Optional[Query]]":
    """Creates a queue of Queries or a sentinel None."""
    queue: asyncio.Queue[Optional[Query]] = asyncio.Queue()
    return queue


@pytest.fixture
def running_queries() -> "asyncio.Queue[str]":
    """Creates a queue of query task IDs."""
    queue: asyncio.Queue[str] = asyncio.Queue()
    return queue


@pytest.fixture
def query_slot() -> asyncio.Semaphore:
    """Creates a semaphore to limit query concurrency."""
    semaphore = asyncio.Semaphore(1)
    return semaphore


@pytest.fixture
def query(explore: Explore, dimension: Dimension) -> Query:
    return Query(explore, (dimension,), query_id="12345")


async def test_compile_explore_without_dimensions_should_not_work(
    explore: Explore, validator: SqlValidator
):
    with pytest.raises(AttributeError):
        await validator.compile_explore(explore)


async def test_compile_explore_compiles_sql(
    mocked_api: respx.MockRouter,
    explore: Explore,
    dimension: Dimension,
    validator: SqlValidator,
):
    query_id = 12345
    sql = "SELECT * FROM users"
    explore.dimensions = [dimension]
    mocked_api.post("queries", params={"fields": "id"}, name="create_query").respond(
        200, json={"id": query_id}
    )
    mocked_api.get(f"queries/{query_id}/run/sql", name="run_query").respond(
        200, text=sql
    )
    compiled = await validator.compile_explore(explore)
    assert compiled.explore_name == explore.name
    assert compiled.model_name == explore.model_name
    assert compiled.sql == sql
    assert compiled.dimension_name is None
    mocked_api["create_query"].calls.assert_called_once()
    mocked_api["run_query"].calls.assert_called_once()


async def test_compile_dimension_compiles_sql(
    mocked_api: respx.MockRouter,
    dimension: Dimension,
    validator: SqlValidator,
):
    query_id = 12345
    sql = "SELECT * FROM users"
    mocked_api.post("queries", params={"fields": "id"}, name="create_query").respond(
        200, json={"id": query_id}
    )
    mocked_api.get(f"queries/{query_id}/run/sql", name="run_query").respond(
        200, text=sql
    )
    compiled = await validator.compile_dimension(dimension)
    assert compiled.explore_name == dimension.explore_name
    assert compiled.model_name == dimension.model_name
    assert compiled.sql == sql
    assert compiled.dimension_name is dimension.name
    mocked_api["create_query"].calls.assert_called_once()
    mocked_api["run_query"].calls.assert_called_once()


async def test_run_query_works(
    mocked_api: respx.MockRouter,
    query: Query,
    validator: SqlValidator,
    queries_to_run: asyncio.Queue,
    running_queries: asyncio.Queue,
    query_slot: asyncio.Semaphore,
):
    query_task_id = "abcdef12345"
    explore_url = "https://spectacles.looker.com/x"

    mocked_api.post(
        "queries", params={"fields": "id,share_url"}, name="create_query"
    ).respond(200, json={"id": query.query_id, "share_url": explore_url})
    mocked_api.post(
        "query_tasks",
        params={"fields": "id", "cache": "false"},
        name="create_query_task",
    ).respond(200, json={"id": query_task_id})

    task = asyncio.create_task(
        validator._run_query(queries_to_run, running_queries, query_slot)
    )

    await queries_to_run.put(query)
    await running_queries.get()
    # Have to manually mark the queue task as done, since normally this is handled by
    # `SqlValidator._get_query_results`
    queries_to_run.task_done()
    query_slot.release()
    await queries_to_run.join()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.gather(task)

    mocked_api["create_query"].calls.assert_called_once()
    mocked_api["create_query_task"].calls.assert_called_once()


async def test_run_query_shuts_down_on_sentinel(
    validator: SqlValidator,
    queries_to_run: asyncio.Queue,
    running_queries: asyncio.Queue,
    query_slot: asyncio.Semaphore,
):
    task = asyncio.create_task(
        validator._run_query(queries_to_run, running_queries, query_slot)
    )

    await queries_to_run.put(None)
    await queries_to_run.join()
    await asyncio.gather(task)


async def test_run_query_handles_exceptions_raised_within(
    mocked_api: respx.MockRouter,
    query: Query,
    validator: SqlValidator,
    queries_to_run: asyncio.Queue,
    running_queries: asyncio.Queue,
    query_slot: asyncio.Semaphore,
):
    query_task_id = "abcdef12345"
    explore_url = "https://spectacles.looker.com/x"

    mocked_api.post(
        "queries", params={"fields": "id,share_url"}, name="create_query"
    ).mock(
        side_effect=tuple(
            [httpx.Response(200, json={"id": query.query_id, "share_url": explore_url})]
            + [
                httpx.Response(404) for _ in range(10)
            ]  # Can't figure out exactly what the right number of 404s is
        )
    )

    mocked_api.post(
        "query_tasks",
        params={"fields": "id", "cache": "false"},
        name="create_query_task",
    ).respond(200, json={"id": query_task_id})

    task = asyncio.create_task(
        validator._run_query(queries_to_run, running_queries, query_slot)
    )

    queries_to_run.put_nowait(query)  # This will succeed
    queries_to_run.put_nowait(query)  # This will fail with 404
    await running_queries.get()  # Retrieve the successfully query

    # Normally these steps are handled by _get_query_results
    queries_to_run.task_done()
    query_slot.release()
    await queries_to_run.join()

    with pytest.raises(LookerApiError):
        await asyncio.gather(task)

    assert running_queries.empty
    mocked_api["create_query"].calls.assert_called()


@pytest.mark.parametrize("fail_fast", (True, False))
async def test_get_query_results_works(
    fail_fast: bool,
    mocked_api: respx.MockRouter,
    validator: SqlValidator,
    queries_to_run: asyncio.Queue,
    running_queries: asyncio.Queue,
    query_slot: asyncio.Semaphore,
):
    mocked_api.get("query_tasks/multi_results", name="get_query_results").respond(
        200, json={}
    )

    query_task_id = "abcdef12345"
    task = asyncio.create_task(
        validator._get_query_results(
            queries_to_run, running_queries, fail_fast, query_slot
        )
    )

    await running_queries.put(query_task_id)
    await running_queries.join()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.gather(task)

    mocked_api["get_query_results"].calls.assert_called_once()


@pytest.mark.parametrize("fail_fast", (True, False))
@patch.object(Query, "divide")
async def test_get_query_results_error_query_is_divided(
    mock_divide: Mock,
    fail_fast: bool,
    mocked_api: respx.MockRouter,
    query: Query,
    validator: SqlValidator,
    queries_to_run: asyncio.Queue,
    running_queries: asyncio.Queue,
    query_slot: asyncio.Semaphore,
):
    query_task_id = "abcdef12345"
    message = "The users table does not exist"
    mocked_api.get("query_tasks/multi_results", name="get_query_results").respond(
        200,
        json={
            query_task_id: {
                "status": "error",
                "data": {
                    "id": query_task_id,
                    "runtime": 460.0,
                    "sql": "SELECT * FROM users",
                    "errors": [
                        {"message": message, "sql_error_loc": {"line": 1, "column": 1}}
                    ],
                },
            }
        },
    )
    # Need more than one dimension so the query will be divided
    query.dimensions = (query.dimensions[0], query.dimensions[0])
    validator._task_to_query[query_task_id] = query

    task = asyncio.create_task(
        validator._get_query_results(
            queries_to_run, running_queries, fail_fast, query_slot
        )
    )

    await queries_to_run.put(query)
    await running_queries.put(query_task_id)
    await running_queries.join()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.gather(task)

    mocked_api["get_query_results"].calls.assert_called_once()
    mock_divide.assert_not_called() if fail_fast else mock_divide.assert_called_once()
    assert query.errored

    # If not fail fast, the explore won't be marked as queried because we haven't yet
    # queried the individual dimensions
    if fail_fast:
        assert query.explore.queried
        assert query.explore.errored
        assert query.explore.errors[0].message == message


@pytest.mark.parametrize("fail_fast", (True, False))
@patch.object(Query, "divide")
async def test_get_query_results_passing_query_is_not_divided(
    mock_divide: Mock,
    fail_fast: bool,
    mocked_api: respx.MockRouter,
    query: Query,
    validator: SqlValidator,
    queries_to_run: asyncio.Queue,
    running_queries: asyncio.Queue,
    query_slot: asyncio.Semaphore,
):
    query_task_id = "abcdef12345"
    mocked_api.get("query_tasks/multi_results", name="get_query_results").respond(
        200,
        json={
            query_task_id: {
                "status": "complete",
                "data": {
                    "id": query_task_id,
                    "runtime": 460.0,
                    "sql": "SELECT * FROM users",
                },
            }
        },
    )
    validator._task_to_query[query_task_id] = query

    task = asyncio.create_task(
        validator._get_query_results(
            queries_to_run, running_queries, fail_fast, query_slot
        )
    )

    await queries_to_run.put(query)
    await running_queries.put(query_task_id)
    await running_queries.join()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.gather(task)

    mocked_api["get_query_results"].calls.assert_called_once()
    mock_divide.assert_not_called()
    assert query.errored is False
    assert query.explore.queried
    assert query in validator._long_running_queries


@pytest.mark.parametrize("fail_fast", (True, False))
async def test_get_query_results_handles_exceptions_raised_within(
    fail_fast: bool,
    mocked_api: respx.MockRouter,
    validator: SqlValidator,
    queries_to_run: asyncio.Queue,
    running_queries: asyncio.Queue,
    query_slot: asyncio.Semaphore,
):
    query_task_id = "abcdef12345"
    mocked_api.get("query_tasks/multi_results", name="get_query_results").respond(404)

    task = asyncio.create_task(
        validator._get_query_results(
            queries_to_run, running_queries, fail_fast, query_slot
        )
    )

    await running_queries.put(query_task_id)
    # Normally we'd let the run_query task pick this up,
    # but since it's not running we'll get it manually
    await queries_to_run.get()
    await running_queries.join()

    with pytest.raises(LookerApiError):
        await asyncio.gather(task)

    assert mocked_api["get_query_results"].call_count == 3


@pytest.mark.parametrize("fail_fast", (True, False))
async def test_search_works_with_passing_query(
    fail_fast: bool,
    mocked_api: respx.MockRouter,
    validator: SqlValidator,
    explore: Explore,
    dimension: Dimension,
):
    explore.dimensions = [dimension, dimension]
    explores = (explore,)

    query_id = 12345
    query_task_id = "abcdef12345"
    explore_url = "https://spectacles.looker.com/x"

    mocked_api.post(
        "queries", params={"fields": "id,share_url"}, name="create_query"
    ).respond(200, json={"id": query_id, "share_url": explore_url})
    mocked_api.post(
        "query_tasks",
        params={"fields": "id", "cache": "false"},
        name="create_query_task",
    ).respond(200, json={"id": query_task_id})
    mocked_api.get("query_tasks/multi_results", name="get_query_results").respond(
        200,
        json={
            query_task_id: {
                "status": "complete",
                "data": {
                    "id": query_task_id,
                    "runtime": 460.0,
                    "sql": "SELECT * FROM users",
                },
            }
        },
    )

    await validator.search(explores, fail_fast)

    mocked_api["create_query"].calls.assert_called_once()
    mocked_api["create_query_task"].calls.assert_called_once()
    mocked_api["get_query_results"].calls.assert_called_once()


@pytest.mark.parametrize("fail_fast", (True, False))
async def test_search_works_with_error_query(
    fail_fast: bool,
    mocked_api: respx.MockRouter,
    validator: SqlValidator,
    explore: Explore,
    dimension: Dimension,
):
    explore.dimensions = [dimension, dimension]
    explores = (explore,)

    explore_url = "https://spectacles.looker.com/x"
    message = "The users table does not exist"

    mocked_api.post(
        "queries", params={"fields": "id,share_url"}, name="create_query"
    ).mock(
        side_effect=(
            httpx.Response(200, json={"id": 1, "share_url": explore_url}),
            httpx.Response(200, json={"id": 2, "share_url": explore_url}),
            httpx.Response(200, json={"id": 3, "share_url": explore_url}),
        )
    )

    mocked_api.post(
        "query_tasks",
        params={"fields": "id", "cache": "false"},
        name="create_query_task",
    ).mock(
        side_effect=(
            httpx.Response(200, json={"id": "abcdef1"}),
            httpx.Response(200, json={"id": "abcdef2"}),
            httpx.Response(200, json={"id": "abcdef3"}),
        )
    )

    mocked_api.get("query_tasks/multi_results", name="get_query_results").mock(
        side_effect=(
            httpx.Response(
                200,
                json={
                    "abcdef1": {
                        "status": "error",
                        "data": {
                            "id": "abcdef1",
                            "runtime": 2.0,
                            "sql": "SELECT * FROM users",
                            "errors": [
                                {
                                    "message": message,
                                    "sql_error_loc": {"line": 1, "column": 1},
                                }
                            ],
                        },
                    }
                },
            ),
            httpx.Response(
                200,
                json={
                    "abcdef2": {
                        "status": "error",
                        "data": {
                            "id": "abcdef2",
                            "runtime": 1.0,
                            "sql": "SELECT age FROM users",
                            "errors": [
                                {
                                    "message": message,
                                    "sql_error_loc": {"line": 1, "column": 1},
                                }
                            ],
                        },
                    },
                    "abcdef3": {
                        "status": "error",
                        "data": {
                            "id": "abcdef3",
                            "runtime": 1.0,
                            "sql": "SELECT user_id FROM users",
                            "errors": [
                                {
                                    "message": message,
                                    "sql_error_loc": {"line": 1, "column": 1},
                                }
                            ],
                        },
                    },
                },
            ),
        )
    )

    await validator.search(explores, fail_fast)

    if fail_fast:
        mocked_api["create_query"].calls.assert_called_once()
        mocked_api["create_query_task"].calls.assert_called_once()
        mocked_api["get_query_results"].calls.assert_called_once()
    else:
        assert mocked_api["create_query"].calls.call_count == 3
        assert mocked_api["create_query_task"].calls.call_count == 3
        assert mocked_api["get_query_results"].calls.call_count == 2

    assert explore.errored
    if fail_fast:
        assert explore.errors[0].message == message
    else:
        assert all(d.errors[0].message == message for d in explore.dimensions)


@pytest.mark.parametrize("fail_fast", (True, False))
async def test_search_handles_exceptions_raised_while_running_queries(
    fail_fast: bool,
    mocked_api: respx.MockRouter,
    validator: SqlValidator,
    explore: Explore,
    dimension: Dimension,
):
    explore.dimensions = [dimension, dimension]
    explores = (explore,)

    mocked_api.post(
        "queries", params={"fields": "id,share_url"}, name="create_query"
    ).respond(404)

    with pytest.raises(LookerApiError):
        await validator.search(explores, fail_fast)

    task_names = (task.get_name() for task in asyncio.all_tasks())
    assert "run_query" not in task_names
    assert "get_query_results" not in task_names


@pytest.mark.parametrize("fail_fast", (True, False))
async def test_search_handles_exceptions_raised_while_getting_query_results(
    fail_fast: bool,
    mocked_api: respx.MockRouter,
    validator: SqlValidator,
    explore: Explore,
    dimension: Dimension,
):
    explore.dimensions = [dimension, dimension]
    explores = (explore,)
    query_id = 12345
    query_task_id = "abcdef12345"
    explore_url = "https://spectacles.looker.com/x"

    mocked_api.post(
        "queries", params={"fields": "id,share_url"}, name="create_query"
    ).respond(200, json={"id": query_id, "share_url": explore_url})
    mocked_api.post(
        "query_tasks",
        params={"fields": "id", "cache": "false"},
        name="create_query_task",
    ).respond(200, json={"id": query_task_id})
    mocked_api.get("query_tasks/multi_results", name="get_query_results").respond(404)

    with pytest.raises(LookerApiError):
        await validator.search(explores, fail_fast)

    task_names = (task.get_name() for task in asyncio.all_tasks())
    assert "run_query" not in task_names
    assert "get_query_results" not in task_names


@pytest.mark.parametrize("fail_fast", (True, False))
async def test_search_with_chunk_size_should_limit_queries(
    fail_fast: bool,
    mocked_api: respx.MockRouter,
    validator: SqlValidator,
    explore: Explore,
    dimension: Dimension,
):
    chunk_size = 10
    explore.dimensions = [dimension] * 100
    explore_url = "https://spectacles.looker.com/x"

    # Define some factories to make IDs sensible across requests
    query_ids = []
    query_task_ids = []

    def create_query_factory(request, route) -> httpx.Response:
        query_id = route.call_count + 1  # Use the call count for incrementing IDs
        query_ids.append(query_id)
        return httpx.Response(200, json={"id": query_id, "share_url": explore_url})

    def create_query_task_factory(request) -> httpx.Response:
        query_id = query_ids.pop()
        query_task_id = f"abcdef{query_id}"
        query_task_ids.append(query_task_id)
        return httpx.Response(200, json={"id": query_task_id})

    def get_query_results_factory(request) -> httpx.Response:
        response = httpx.Response(
            200,
            json={
                query_task_id: {
                    "status": "complete",
                    "data": {
                        "id": query_task_id,
                        "runtime": 0.0,
                        "sql": "SELECT * FROM users",
                    },
                }
                for query_task_id in query_task_ids
            },
        )
        return response

    mocked_api.post(
        "queries", params={"fields": "id,share_url"}, name="create_query"
    ).mock(side_effect=create_query_factory)
    mocked_api.post(
        "query_tasks",
        params={"fields": "id", "cache": "false"},
        name="create_query_task",
    ).mock(side_effect=create_query_task_factory)
    mocked_api.get("query_tasks/multi_results", name="get_query_results").mock(
        side_effect=get_query_results_factory
    )

    await validator.search(
        explores=(explore,), fail_fast=fail_fast, chunk_size=chunk_size
    )
    assert mocked_api["create_query"].call_count == 10
    assert mocked_api["create_query_task"].call_count == 10
    assert mocked_api["get_query_results"].call_count > 0
    request: httpx.Request
    for request, _ in mocked_api["create_query"].calls:
        body = json.loads(request.content)
        assert len(body["fields"]) == 10


async def test_search_with_explore_without_dimensions_warns(
    explore: Explore, validator: SqlValidator, caplog: pytest.LogCaptureFixture
):
    caplog.set_level(logging.WARNING)
    explore.dimensions = []
    await validator.search(explores=(explore,), fail_fast=False)
    assert explore.name in caplog.text


async def test_looker_api_error_with_queries_in_flight_shuts_down_gracefully(
    mocked_api: respx.MockRouter,
    validator: SqlValidator,
    explore: Explore,
    dimension: Dimension,
):
    chunk_size = 10
    explore.dimensions = [dimension] * 1000
    explore_url = "https://spectacles.looker.com/x"

    # Define some factories to make IDs sensible across requests
    query_ids = []
    query_task_ids = []

    def create_query_factory(request, route) -> httpx.Response:
        query_id = route.call_count + 1  # Use the call count for incrementing IDs
        query_ids.append(query_id)
        return httpx.Response(200, json={"id": query_id, "share_url": explore_url})

    def create_query_task_factory(request) -> httpx.Response:
        query_id = query_ids[0]
        if query_id == 26:
            return httpx.Response(502, text="502: Bad Gateway")
        else:
            query_ids.pop()
            query_task_id = f"abcdef{query_id}"
            query_task_ids.append(query_task_id)
            return httpx.Response(200, json={"id": query_task_id})

    def get_query_results_factory(request) -> httpx.Response:
        response = httpx.Response(
            200,
            json={
                query_task_id: {
                    "status": "complete",
                    "data": {
                        "id": query_task_id,
                        "runtime": 0.0,
                        "sql": "SELECT * FROM users",
                    },
                }
                for query_task_id in query_task_ids
            },
        )
        return response

    mocked_api.post(
        "queries", params={"fields": "id,share_url"}, name="create_query"
    ).mock(side_effect=create_query_factory)
    mocked_api.post(
        "query_tasks",
        params={"fields": "id", "cache": "false"},
        name="create_query_task",
    ).mock(side_effect=create_query_task_factory)
    mocked_api.get("query_tasks/multi_results", name="get_query_results").mock(
        side_effect=get_query_results_factory
    )

    with pytest.raises(LookerApiError):
        await validator.search(
            explores=(explore,), fail_fast=False, chunk_size=chunk_size
        )
