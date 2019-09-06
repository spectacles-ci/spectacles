import json
import asyncio
from pathlib import Path
from unittest.mock import Mock, patch
import asynctest
import requests
import aiohttp
import pytest
from fonz.connection import Fonz
from fonz.lookml import Project, Model, Explore, Dimension
from fonz.exceptions import SqlError, ConnectionError, FonzException


def load(filename):
    """Helper method to load a JSON file from tests/resources and parse it."""
    path = Path(__file__).parent / "resources" / filename
    with path.open() as file:
        return json.load(file)


@pytest.fixture
def client():
    client = Fonz(
        url="https://test.looker.com",
        client_id="CLIENT_ID",
        client_secret="CLIENT_SECRET",
        port=19999,
        api=3.1,
    )
    client.project = "test_project"
    return client


@pytest.fixture
def lookml():
    dimensions = [
        Dimension(
            "test_view.dimension_one",
            "number",
            "${TABLE}.dimension_one",
            "/projects/fonz/files/test_view.view.lkml?line=340",
        ),
        Dimension(
            "test_view.dimension_two",
            "number",
            "${TABLE}.dimension_two",
            "/projects/fonz/files/test_view.view.lkml?line=360",
        ),
    ]
    explores_model_one = [Explore("test_explore_one", dimensions)]
    explores_model_two = [Explore("test_explore_two", dimensions)]
    models = [
        Model("test_model_one", "test_project", explores_model_one),
        Model("test_model_two", "test_project", explores_model_two),
    ]
    project = Project("test_project", models)
    return project


def test_fonz_with_no_client_id_raises_exception():
    with pytest.raises(FonzException):
        Fonz(
            url="https://test.looker.com",
            client_id=None,
            client_secret="CLIENT_SECRET",
            port=19999,
            api=3.1,
        )


def test_fonz_with_no_client_secret_raises_exception():
    with pytest.raises(FonzException):
        Fonz(
            url="https://test.looker.com",
            client_id="CLIENT_ID",
            client_secret=None,
            port=19999,
            api=3.1,
        )


@patch("fonz.connection.requests.Session.post")
def test_connect_sets_session_headers_correctly(mock_post, client):
    mock_post.return_value.json.return_value = {"access_token": "ACCESS_TOKEN"}
    client.connect()
    assert client.session.headers == {"Authorization": "token ACCESS_TOKEN"}


@patch("fonz.connection.requests.Session.post")
def test_connect_bad_request_raises_connection_error(mock_post, client):
    mock_response = requests.models.Response()
    mock_response.status_code = 404
    mock_post.return_value = mock_response
    with pytest.raises(ConnectionError):
        client.connect()


@patch("fonz.connection.requests.Session.patch")
@patch("fonz.connection.requests.Session.put")
def test_update_session_patch_with_bad_request_raises_connection_error(
    mock_put, mock_patch, client
):
    mock_response = requests.models.Response()
    mock_response.status_code = 404
    mock_patch.return_value = mock_response
    with pytest.raises(ConnectionError):
        client.update_session("test_project", "test_branch")


@patch("fonz.connection.requests.Session.patch")
@patch("fonz.connection.requests.Session.put")
def test_update_session_put_with_bad_request_raises_connection_error(
    mock_put, mock_patch, client
):
    mock_response = requests.models.Response()
    mock_response.status_code = 404
    mock_put.return_value = mock_response
    with pytest.raises(ConnectionError):
        client.update_session("test_project", "test_branch")


@patch("fonz.connection.Fonz.get_dimensions")
@patch("fonz.connection.Fonz.get_models")
def test_build_project(mock_get_models, mock_get_dimensions, lookml, client):
    mock_get_models.return_value = load("response_models.json")
    mock_get_dimensions.return_value = load("response_dimensions.json")
    client.build_project(selectors=["*.*"])
    assert client.lookml == lookml


def test_count_explores(client, lookml):
    client.lookml = lookml
    count = client.count_explores()
    assert count == 2


@patch("fonz.connection.requests.Session.get")
def test_get_dimensions_with_bad_request_raises_exception(mock_get, client):
    mock_response = requests.models.Response()
    mock_response.status_code = 404
    mock_get.return_value = mock_response
    with pytest.raises(FonzException):
        client.get_dimensions("test_model", "test_explore_one")


@patch("fonz.connection.requests.Session.get")
def test_get_models_with_bad_request_raises_exception(mock_get, client):
    mock_response = requests.models.Response()
    mock_response.status_code = 404
    mock_get.return_value = mock_response
    with pytest.raises(FonzException):
        client.get_models()


def test_validate_before_lookml_built(client):
    client.lookml = None
    with pytest.raises(FonzException):
        client.validate()


@asynctest.patch("fonz.connection.Fonz.query_dimension")
@asynctest.patch("fonz.connection.Fonz.query_explore")
def test_validate_explore_batch_calls_explore_query(
    mock_query_explore, mock_query_dimension, client, lookml
):
    model = lookml.models[0]
    explore = model.explores[0]

    client.validate_explore(model, explore, batch=True)
    mock_query_explore.assert_called_once()
    assert mock_query_dimension.call_count == 0


@asynctest.patch("fonz.connection.Fonz.query_dimension")
@asynctest.patch("fonz.connection.Fonz.query_explore")
def test_validate_explore_no_batch_calls_dimension_query(
    mock_query_explore, mock_query_dimension, client, lookml
):
    model = lookml.models[0]
    explore = model.explores[0]

    client.validate_explore(model, explore, batch=False)
    assert mock_query_dimension.call_count > 1
    assert mock_query_explore.call_count == 0


@pytest.mark.asyncio
@asynctest.patch("aiohttp.ClientSession.post")
async def test_create_query(mock_post, client):
    QUERY_ID = 124950204921
    mock_post.return_value.__aenter__.return_value.json = asynctest.CoroutineMock(
        return_value={"id": QUERY_ID}
    )
    async with aiohttp.ClientSession() as session:
        query_id = await client.create_query(
            session,
            "test_model",
            "test_explore_one",
            ["dimension_one", "dimension_two"],
        )
    assert query_id == QUERY_ID
    mock_post.assert_called_once_with(
        url="https://test.looker.com:19999/api/3.1/queries",
        json={
            "model": "test_model",
            "view": "test_explore_one",
            "fields": ["dimension_one", "dimension_two"],
            "limit": 1,
        },
    )


@pytest.mark.asyncio
@asynctest.patch("aiohttp.ClientSession.post")
async def test_run_query(mock_post, client):
    QUERY_ID = 124950204921
    QUERY_TASK_ID = "a1ds2d49d5d02wdf0we4a921e"
    mock_post.return_value.__aenter__.return_value.json = asynctest.CoroutineMock(
        return_value={"id": QUERY_TASK_ID}
    )
    async with aiohttp.ClientSession() as session:
        query_task_id = await client.run_query(session, QUERY_ID)
    assert query_task_id == QUERY_TASK_ID
    mock_post.assert_called_once_with(
        url="https://test.looker.com:19999/api/3.1/query_tasks",
        json={"query_id": QUERY_ID, "result_format": "json"},
    )


@pytest.mark.asyncio
@asynctest.patch("fonz.connection.Fonz.get_query_results")
@asynctest.patch("fonz.connection.Fonz.run_query")
@asynctest.patch("fonz.connection.Fonz.create_query")
async def test_query_dimension_failure_sets_errors_on_lookml_objects(
    mock_create, mock_run, mock_get_results, client, lookml
):
    QUERY_ID = 124950204921
    QUERY_TASK_ID = "a1ds2d49d5d02wdf0we4a921e"
    mock_create.return_value = QUERY_ID
    mock_run.return_value = QUERY_TASK_ID
    error_result = {
        "errors": [
            {"message_details": "An error message.", "sql_error_loc": {"line": 12}}
        ],
        "sql": "SELECT something FROM something",
    }
    mock_get_results.return_value = error_result

    model = lookml.models[0]
    explore = model.explores[0]
    dimension = explore.dimensions[0]

    await client.query_dimension(model, explore, dimension)

    mock_create.assert_called_once()
    mock_run.assert_called_once()
    mock_get_results.assert_called_once()

    assert dimension.errored
    assert explore.errored
    assert model.errored
    assert isinstance(dimension.error, SqlError)
    assert dimension.error.message == "An error message."
    assert dimension.error.line_number == 12


@pytest.mark.asyncio
@asynctest.patch("fonz.connection.Fonz.get_query_results")
@asynctest.patch("fonz.connection.Fonz.run_query")
@asynctest.patch("fonz.connection.Fonz.create_query")
async def test_query_dimension_success_does_not_set_errors_on_lookml_objects(
    mock_create, mock_run, mock_get_results, client, lookml
):
    QUERY_ID = 124950204921
    QUERY_TASK_ID = "a1ds2d49d5d02wdf0we4a921e"
    mock_create.return_value = QUERY_ID
    mock_run.return_value = QUERY_TASK_ID
    mock_get_results.return_value = {"dimension_one": "A returned value."}

    model = lookml.models[0]
    explore = model.explores[0]
    dimension = explore.dimensions[0]

    await client.query_dimension(model, explore, dimension)

    mock_create.assert_called_once()
    mock_run.assert_called_once()
    mock_get_results.assert_called_once()

    assert not dimension.errored
    assert not explore.errored
    assert not model.errored
    assert not dimension.error


@pytest.mark.asyncio
@asynctest.patch("fonz.connection.Fonz.get_query_results")
@asynctest.patch("fonz.connection.Fonz.run_query")
@asynctest.patch("fonz.connection.Fonz.create_query")
async def test_query_explore_failure_sets_errors_on_lookml_objects(
    mock_create, mock_run, mock_get_results, client, lookml
):
    QUERY_ID = 124950204921
    QUERY_TASK_ID = "a1ds2d49d5d02wdf0we4a921e"
    mock_create.return_value = QUERY_ID
    mock_run.return_value = QUERY_TASK_ID
    error_result = {
        "errors": [
            {"message_details": "An error message.", "sql_error_loc": {"line": 12}}
        ],
        "sql": "SELECT something FROM something",
    }
    mock_get_results.return_value = error_result

    model = lookml.models[0]
    explore = model.explores[0]

    await client.query_explore(model, explore)

    mock_create.assert_called_once()
    mock_run.assert_called_once()
    mock_get_results.assert_called_once()

    assert explore.errored
    assert model.errored
    assert isinstance(explore.error, SqlError)
    assert explore.error.message == "An error message."
    # Account for extra line number added by Looker comment
    assert explore.error.line_number == 11


@pytest.mark.asyncio
@asynctest.patch("fonz.connection.Fonz.get_query_results")
@asynctest.patch("fonz.connection.Fonz.run_query")
@asynctest.patch("fonz.connection.Fonz.create_query")
async def test_query_explore_success_does_not_set_errors_on_lookml_objects(
    mock_create, mock_run, mock_get_results, client, lookml
):
    QUERY_ID = 124950204921
    QUERY_TASK_ID = "a1ds2d49d5d02wdf0we4a921e"
    mock_create.return_value = QUERY_ID
    mock_run.return_value = QUERY_TASK_ID
    mock_get_results.return_value = {"test_explore_one": "A returned value."}

    model = lookml.models[0]
    explore = model.explores[0]

    await client.query_explore(model, explore)

    mock_create.assert_called_once()
    mock_run.assert_called_once()
    mock_get_results.assert_called_once()

    assert not explore.errored
    assert not model.errored
    assert not explore.error


@patch("fonz.connection.Fonz.validate_explore")
def test_validate_event_loop_is_closed_on_finish(mock_validate_explore, client, lookml):
    client.lookml = lookml
    client.validate()
    with pytest.raises(RuntimeError):
        asyncio.get_running_loop()
