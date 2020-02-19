from pathlib import Path
import json
from unittest.mock import patch, Mock
import pytest
from spectacles.lookml import Project, Model, Explore, Dimension
from spectacles.client import LookerClient
from spectacles.validators import SqlValidator, Query, QueryResult
from spectacles.exceptions import SqlError

TEST_BASE_URL = "https://test.looker.com"
TEST_CLIENT_ID = "test_client_id"
TEST_CLIENT_SECRET = "test_client_secret"


def load(filename):
    """Helper method to load a JSON file from tests/resources and parse it."""
    path = Path(__file__).parent / "resources" / filename
    with path.open() as file:
        return json.load(file)


@pytest.fixture
def client(monkeypatch):
    mock_authenticate = Mock(spec=LookerClient.authenticate)
    monkeypatch.setattr(LookerClient, "authenticate", mock_authenticate)
    return LookerClient(TEST_BASE_URL, TEST_CLIENT_ID, TEST_CLIENT_SECRET)


@pytest.fixture
def validator(client):
    return SqlValidator(client=client, project="test_project")


@pytest.fixture
def project():
    dimensions = [
        Dimension(
            "test_view.dimension_one",
            "number",
            "${TABLE}.dimension_one",
            (
                "https://test.looker.com/projects/spectacles/"
                "files/test_view.view.lkml?line=340"
            ),
        ),
        Dimension(
            "test_view.dimension_two",
            "number",
            "${TABLE}.dimension_two",
            (
                "https://test.looker.com/projects/spectacles/"
                "files/test_view.view.lkml?line=360"
            ),
        ),
    ]
    explores_model_one = [Explore("test_explore_one", dimensions)]
    explores_model_two = [Explore("test_explore_two", dimensions)]
    models = [
        Model("test_model_one", "test_project", explores_model_one),
        Model("test_model.two", "test_project", explores_model_two),
    ]
    project = Project("test_project", models)
    return project


@patch("spectacles.client.LookerClient.get_lookml_dimensions")
@patch("spectacles.client.LookerClient.get_lookml_models")
def test_build_project(mock_get_models, mock_get_dimensions, project, validator):
    mock_get_models.return_value = load("response_models.json")
    mock_get_dimensions.return_value = load("response_dimensions.json")
    validator.build_project(selectors=["*/*"])
    assert validator.project == project


def test_error_is_set_on_project(project, validator):
    """
    If get_query_results returns an error for a mapped query task ID,
    The corresponding explore should be set to errored and
    The SqlError instance should be present and validated

    TODO: Refactor error responses into fixtures
    TODO: Should query IDs be ints instead of strings?

    """
    query_task_id = "akdk13kkidi2mkv029rld"
    message = "An error has occurred"
    sql = "SELECT DISTINCT 1 FROM table_name"
    error_details = {"message": message, "sql": sql}
    validator.project = project
    explore = project.models[0].explores[0]
    query = Query(query_id="10319", lookml_ref=explore, query_task_id=query_task_id)
    validator._running_queries.append(query)
    query_result = QueryResult(query_task_id, status="error", error=error_details)
    validator._query_by_task_id[query_task_id] = query
    returned_sql_error = validator._handle_query_result(query_result)
    expected_sql_error = SqlError(
        path="test_explore_one", url=None, message=message, sql=sql
    )
    assert returned_sql_error == expected_sql_error
    assert returned_sql_error == explore.error
    assert explore.queried
    assert explore.errored
    assert not validator._running_queries
    assert validator.project.errored
    assert validator.project.models[0].errored
    # Batch mode, so none of the dimensions should have errored set
    assert not any(dimension.errored for dimension in explore.dimensions)
    assert all(dimension.queried for dimension in explore.dimensions)


@patch('spectacles.validators.LookerClient.cancel_query_task')
def test_cancel_queries(mock_client_cancel, validator):
    """
    Cancelling queries should result in the same number of client calls as
    query tasks IDs passed in, with the corresponding query task IDs called

    TODO: Use the correct mock method for testing how many times called for each

    """
    query_task_ids = ['A', 'B', 'C']
    validator._cancel_queries(query_task_ids)
    for task_id in query_task_ids:
        mock_client_cancel.assert_any_call(task_id)


def test_handle_running_query(validator):
    query_task_id = 'sakgwj392jfkajgjcks'
    query = Query(
        query_id='19428',
        lookml_ref=Dimension('dimension_one', 'string', '${TABLE}.dimension_one'),
        query_task_id=query_task_id
    )
    query_result = QueryResult(query_task_id=query_task_id, status='running')
    validator._running_queries = [query]
    validator._query_by_task_id[query_task_id] = query
    returned_sql_error = validator._handle_query_result(query_result)

    assert validator._running_queries == [query]
    assert not returned_sql_error


def test_count_explores(validator, project):
    validator.project = project
    assert validator._count_explores() == 2

    explore = validator.project.models[0].explores[0]
    validator.project.models[0].explores.extend([explore, explore])
    assert validator._count_explores() == 4

def test_extract_error_details_error_dict(validator):
    message = "An error message."
    message_details = "Shocking details."
    sql = "SELECT * FROM orders"
    query_result = {
        "status": "error",
        "data": {
            "errors": [{"message": message, "message_details": message_details}],
            "sql": sql,
        },
    }
    extracted = validator._extract_error_details(query_result)
    assert extracted["message"] == f"{message} {message_details}"
    assert extracted["sql"] == sql


def test_extract_error_details_error_list(validator):
    message = "An error message."
    query_result = {"status": "error", "data": [message]}
    extracted = validator._extract_error_details(query_result)
    assert extracted["message"] == message
    assert extracted["sql"] is None


def test_extract_error_details_error_other(validator):
    query_result = {"status": "error", "data": "some string"}
    with pytest.raises(TypeError):
        validator._extract_error_details(query_result)


def test_extract_error_details_error_non_str_message_details(validator):
    message = {"message": "An error messsage.", "details": "More details."}
    sql = "SELECT * FROM orders"
    query_result = {
        "status": "error",
        "data": {"errors": [{"message_details": message}], "sql": sql},
    }
    with pytest.raises(TypeError):
        validator._extract_error_details(query_result)


def test_extract_error_details_no_message_details(validator):
    message = "An error message."
    query_result = {
        "status": "error",
        "data": {"errors": [{"message": message, "message_details": None}]},
    }
    extracted = validator._extract_error_details(query_result)
    assert extracted["message"] == message
    assert extracted["sql"] is None


def test_extract_error_details_error_loc_wo_line(validator):
    message = "An error message."
    sql = "SELECT x FROM orders"
    query_result = {
        "status": "error",
        "data": {
            "errors": [{"message": message, "sql_error_loc": {"character": 8}}],
            "sql": sql,
        },
    }
    extracted = validator._extract_error_details(query_result)
    assert extracted["message"] == message
    assert extracted["sql"] == sql
