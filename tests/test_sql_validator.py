from pathlib import Path
import json
from collections import defaultdict
from unittest.mock import patch, create_autospec, Mock
import pytest
from spectacles.lookml import Project, Model, Explore, Dimension
from spectacles.client import LookerClient
from spectacles.validators import SqlValidator, Query, QueryResult
from spectacles.exceptions import SqlError, SpectaclesException

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
    explores_model_two = [
        Explore("test_explore_one", dimensions),
        Explore("test_explore_two", dimensions),
    ]
    models = [
        Model("test_model_one", "test_project", explores_model_one),
        Model("test_model.two", "test_project", explores_model_two),
    ]
    project = Project("test_project", models)
    return project


def test_parse_selectors_handles_duplicates():
    expected = defaultdict(set, model_one=set(["explore_one"]))
    assert (
        SqlValidator.parse_selectors(["model_one/explore_one", "model_one/explore_one"])
        == expected
    )


def test_parse_selectors_handles_same_explore_different_model():
    expected = defaultdict(
        set, model_one=set(["explore_one"]), model_two=set(["explore_one"])
    )
    assert (
        SqlValidator.parse_selectors(["model_one/explore_one", "model_two/explore_one"])
        == expected
    )


def test_parse_selectors_bad_format_raises_error():
    with pytest.raises(SpectaclesException):
        SqlValidator.parse_selectors(["model_one.explore_one", "model_two:explore_one"])


@patch("spectacles.client.LookerClient.get_lookml_dimensions")
@patch("spectacles.client.LookerClient.get_lookml_models")
def test_build_project(mock_get_models, mock_get_dimensions, project, validator):
    mock_get_models.return_value = load("response_models.json")
    mock_get_dimensions.return_value = load("response_dimensions.json")
    validator.build_project(selectors=["*/*"], exclusions=[])
    assert validator.project == project


@patch("spectacles.client.LookerClient.get_lookml_dimensions")
@patch("spectacles.client.LookerClient.get_lookml_models")
def test_build_project_all_models_excluded(
    mock_get_models, mock_get_dimensions, project, validator
):
    mock_get_models.return_value = load("response_models.json")
    mock_get_dimensions.return_value = load("response_dimensions.json")
    validator.build_project(
        selectors=["*/*"], exclusions=["test_model_one/*", "test_model.two/*"]
    )
    project.models = []
    assert validator.project == project


@patch("spectacles.client.LookerClient.get_lookml_dimensions")
@patch("spectacles.client.LookerClient.get_lookml_models")
def test_build_project_one_model_excluded(
    mock_get_models, mock_get_dimensions, project, validator
):
    mock_get_models.return_value = load("response_models.json")
    mock_get_dimensions.return_value = load("response_dimensions.json")
    validator.build_project(selectors=["*/*"], exclusions=["test_model_one/*"])
    project.models = [
        model for model in project.models if model.name != "test_model_one"
    ]
    assert validator.project == project


@patch("spectacles.client.LookerClient.get_lookml_dimensions")
@patch("spectacles.client.LookerClient.get_lookml_models")
def test_build_project_one_model_selected(
    mock_get_models, mock_get_dimensions, project, validator
):
    mock_get_models.return_value = load("response_models.json")
    mock_get_dimensions.return_value = load("response_dimensions.json")
    validator.build_project(selectors=["test_model.two/*"], exclusions=[])
    project.models = [
        model for model in project.models if model.name == "test_model.two"
    ]
    assert validator.project == project


@patch("spectacles.client.LookerClient.get_lookml_dimensions")
@patch("spectacles.client.LookerClient.get_lookml_models")
def test_build_project_one_explore_excluded(
    mock_get_models, mock_get_dimensions, project, validator
):
    mock_get_models.return_value = load("response_models.json")
    mock_get_dimensions.return_value = load("response_dimensions.json")
    validator.build_project(
        selectors=["*/*"], exclusions=["test_model_one/test_explore_one"]
    )
    project.models = [
        model for model in project.models if model.name != "test_model_one"
    ]
    assert validator.project == project


@patch("spectacles.client.LookerClient.get_lookml_dimensions")
@patch("spectacles.client.LookerClient.get_lookml_models")
def test_build_project_one_explore_selected(
    mock_get_models, mock_get_dimensions, project, validator
):
    mock_get_models.return_value = load("response_models.json")
    mock_get_dimensions.return_value = load("response_dimensions.json")
    validator.build_project(
        selectors=["test_model.two/test_explore_two"], exclusions=[]
    )
    project.models = [
        model for model in project.models if model.name == "test_model.two"
    ]
    project.models[0].explores = [
        explore
        for explore in project.models[0].explores
        if explore.name == "test_explore_two"
    ]
    assert validator.project == project


@patch("spectacles.client.LookerClient.get_lookml_dimensions")
@patch("spectacles.client.LookerClient.get_lookml_models")
def test_build_project_one_ambiguous_explore_excluded(
    mock_get_models, mock_get_dimensions, project, validator
):
    mock_get_models.return_value = load("response_models.json")
    mock_get_dimensions.return_value = load("response_dimensions.json")
    validator.build_project(
        selectors=["*/*"], exclusions=["test_model.two/test_explore_one"]
    )
    for model in project.models:
        if model.name == "test_model.two":
            model.explores = [
                explore
                for explore in model.explores
                if explore.name != "test_explore_one"
            ]
    assert validator.project == project


@patch("spectacles.client.LookerClient.create_query")
def test_create_explore_query(mock_create_query, project, validator):
    query_id = 123
    query_url = "https://example.looker.com/x/12345"
    mock_create_query.return_value = {"id": query_id, "share_url": query_url}
    model = project.models[0]
    explore = model.explores[0]
    query = validator._create_explore_query(explore, model.name)

    expected_result = Query(query_id, explore, query_url)

    assert query.query_id == expected_result.query_id
    assert query.lookml_ref == expected_result.lookml_ref
    assert query.query_url == expected_result.query_url


def test_get_running_query_tasks(validator):
    queries = [
        Query(
            query_id="12345",
            lookml_ref=None,
            query_task_id="abc",
            query_url="https://example.looker.com/x/12345",
        ),
        Query(
            query_id="67890",
            lookml_ref=None,
            query_task_id="def",
            query_url="https://example.looker.com/x/67890",
        ),
    ]
    validator._running_queries = queries
    assert validator.get_running_query_tasks() == ["abc", "def"]


def test_validate_hybrid_mode_no_errors_does_not_repeat(validator):
    mock_run: Mock = create_autospec(validator._create_and_run)
    validator.project.errored = False
    validator._create_and_run = mock_run
    validator.validate(mode="hybrid")
    validator._create_and_run.assert_called_once_with(mode="hybrid")


def test_validate_hybrid_mode_with_errors_does_repeat(validator):
    mock_run: Mock = create_autospec(validator._create_and_run)
    validator.project.errored = True
    validator._create_and_run = mock_run
    validator.validate(mode="hybrid")
    validator._create_and_run.call_count == 2


def test_create_and_run_keyboard_interrupt_cancels_queries(validator):
    validator._running_queries = [
        Query(
            query_id="12345",
            lookml_ref=None,
            query_task_id="abc",
            query_url="https://example.looker.com/x/12345",
        )
    ]
    mock_create_queries = create_autospec(validator._create_queries)
    mock_create_queries.side_effect = KeyboardInterrupt()
    validator._create_queries = mock_create_queries
    mock_cancel_queries = create_autospec(validator._cancel_queries)
    validator._cancel_queries = mock_cancel_queries
    try:
        validator._create_and_run(mode="batch")
    except SpectaclesException:
        mock_cancel_queries.assert_called_once_with(query_task_ids=["abc"])


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
    query_url = "https://example.looker.com/x/12345"
    query = Query(
        query_id="10319",
        lookml_ref=explore,
        query_task_id=query_task_id,
        query_url=query_url,
    )
    validator._running_queries.append(query)
    query_result = QueryResult(query_task_id, status="error", error=error_details)
    validator._query_by_task_id[query_task_id] = query
    returned_sql_error = validator._handle_query_result(query_result)
    expected_sql_error = SqlError(
        path="test_explore_one", url=None, message=message, sql=sql, query_url=query_url
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


@patch("spectacles.validators.LookerClient.cancel_query_task")
def test_cancel_queries(mock_client_cancel, validator):
    """
    Cancelling queries should result in the same number of client calls as
    query tasks IDs passed in, with the corresponding query task IDs called.

    """
    query_task_ids = ["A", "B", "C"]
    validator._cancel_queries(query_task_ids)
    for task_id in query_task_ids:
        mock_client_cancel.assert_any_call(task_id)


def test_handle_running_query(validator):
    query_task_id = "sakgwj392jfkajgjcks"
    query = Query(
        query_id="19428",
        lookml_ref=Dimension("dimension_one", "string", "${TABLE}.dimension_one"),
        query_task_id=query_task_id,
        query_url="https://example.looker.com/x/12345",
    )
    query_result = QueryResult(query_task_id=query_task_id, status="running")
    validator._running_queries = [query]
    validator._query_by_task_id[query_task_id] = query
    returned_sql_error = validator._handle_query_result(query_result)

    assert validator._running_queries == [query]
    assert not returned_sql_error


def test_count_explores(validator, project):
    validator.project = project
    assert validator._count_explores() == 3

    explore = validator.project.models[0].explores[0]
    validator.project.models[0].explores.extend([explore, explore])
    assert validator._count_explores() == 5


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
