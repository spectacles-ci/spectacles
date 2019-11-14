from pathlib import Path
import json
from unittest.mock import patch, Mock
import pytest
from spectacles.lookml import Project, Model, Explore, Dimension
from spectacles.client import LookerClient
from spectacles.validators import SqlValidator
from spectacles.exceptions import SpectaclesException

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


@patch("spectacles.client.LookerClient.get_query_task_multi_results")
def test_get_query_results_task_running(mock_get_query_task_multi_results, validator):
    mock_response = {"status": "running"}
    mock_get_query_task_multi_results.return_value = {"query_task_a": mock_response}
    still_running, errors = validator._get_query_results(["query_task_a"])
    assert not errors
    assert still_running == ["query_task_a"]


@patch("spectacles.client.LookerClient.get_query_task_multi_results")
def test_get_query_results_task_complete(
    mock_get_query_task_multi_results, validator, project
):
    lookml_object = project.models[0].explores[0]
    validator.query_tasks = {"query_task_a": lookml_object}
    mock_response = {"status": "complete"}
    mock_get_query_task_multi_results.return_value = {"query_task_a": mock_response}
    still_running, errors = validator._get_query_results(["query_task_a"])
    assert not errors
    assert not still_running


@patch("spectacles.client.LookerClient.get_query_task_multi_results")
def test_get_query_results_task_error_dict(
    mock_get_query_task_multi_results, validator, project
):
    lookml_object = project.models[0].explores[0]
    validator.query_tasks = {"query_task_a": lookml_object}
    mock_message = "An error message."
    mock_sql = "SELECT * FROM orders"
    mock_response = {
        "status": "error",
        "data": {"errors": [{"message_details": mock_message}], "sql": mock_sql},
    }
    mock_get_query_task_multi_results.return_value = {"query_task_a": mock_response}
    still_running, errors = validator._get_query_results(["query_task_a"])
    assert errors[0].path == lookml_object.name
    assert errors[0].message == mock_message
    assert errors[0].sql == mock_sql
    assert not still_running


@patch("spectacles.client.LookerClient.get_query_task_multi_results")
def test_get_query_results_task_error_list(
    mock_get_query_task_multi_results, validator, project
):
    lookml_object = project.models[0].explores[0]
    validator.query_tasks = {"query_task_a": lookml_object}
    mock_message = "An error message."
    mock_response = {"status": "error", "data": [mock_message]}
    mock_get_query_task_multi_results.return_value = {"query_task_a": mock_response}
    still_running, errors = validator._get_query_results(["query_task_a"])
    assert errors[0].path == lookml_object.name
    assert errors[0].message == mock_message
    assert errors[0].sql is None
    assert not still_running


@patch("spectacles.client.LookerClient.get_query_task_multi_results")
def test_get_query_results_task_error_other(
    mock_get_query_task_multi_results, validator, project
):
    lookml_object = project.models[0].explores[0]
    validator.query_tasks = {"query_task_a": lookml_object}
    mock_response = {"status": "error", "data": "some string"}
    mock_get_query_task_multi_results.return_value = {"query_task_a": mock_response}
    with pytest.raises(SpectaclesException):
        still_running, errors = validator._get_query_results(["query_task_a"])


@patch("spectacles.client.LookerClient.get_query_task_multi_results")
def test_get_query_results_non_str_message_details(
    mock_get_query_task_multi_results, validator, project
):
    lookml_object = project.models[0].explores[0]
    validator.query_tasks = {"query_task_a": lookml_object}
    mock_message = {"message": "An error messsage.", "details": "More details."}
    mock_sql = "SELECT * FROM orders"
    mock_response = {
        "status": "error",
        "data": {"errors": [{"message_details": mock_message}], "sql": mock_sql},
    }
    mock_get_query_task_multi_results.return_value = {"query_task_a": mock_response}
    with pytest.raises(SpectaclesException):
        still_running, errors = validator._get_query_results(["query_task_a"])
