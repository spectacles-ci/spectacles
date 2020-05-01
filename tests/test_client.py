from typing import List, Tuple, Callable
import os
import inspect
import pytest
from unittest.mock import patch, Mock
import requests
from spectacles.client import LookerClient
from spectacles.exceptions import SpectaclesException, ApiConnectionError


def get_client_method_names() -> List[str]:
    """Extracts method names from LookerClient to test for bad responses"""
    client_members: List[Tuple[str, Callable]] = inspect.getmembers(
        LookerClient, predicate=inspect.isroutine
    )
    client_methods: List[str] = [
        member[0] for member in client_members if not member[0].startswith("__")
    ]
    for skip_method in ("authenticate", "cancel_query_task"):
        client_methods.remove(skip_method)
    return client_methods


@pytest.fixture
def mock_404_response():
    mock = Mock(spec=requests.Response)
    mock.status_code = 404
    mock.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "An HTTP error occurred."
    )
    return mock


@pytest.fixture
def client_kwargs():
    return dict(
        authenticate={"client_id": "", "client_secret": "", "api_version": 3.1},
        get_looker_release_version={},
        update_session={"project": "project_name", "branch": "branch_name"},
        all_lookml_tests={"project": "project_name"},
        run_lookml_test={"project": "project_name"},
        get_lookml_models={},
        get_lookml_dimensions={"model": "model_name", "explore": "explore_name"},
        create_query={
            "model": "model_name",
            "explore": "explore_name",
            "dimensions": ["dimension_a", "dimension_b"],
        },
        create_query_task={"query_id": 13041},
        get_query_task_multi_results={"query_task_ids": ["ajsdkgj", "askkwk"]},
    )


@pytest.mark.vcr
def test_get_looker_release_version_should_return_correct_version(looker_client):
    version = looker_client.get_looker_release_version()
    assert version == "7.6.17"


@pytest.mark.vcr(filter_post_data_parameters=["client_id", "client_secret"])
def test_bad_authentication_request_should_raise_api_error():
    with pytest.raises(ApiConnectionError):
        LookerClient(
            base_url="https://spectacles.looker.com",
            client_id=os.environ.get("LOOKER_CLIENT_ID"),
            client_secret="xxxxxxxxxxxxxx",
        )


@pytest.mark.vcr(filter_post_data_parameters=["client_id", "client_secret"])
def test_unsupported_api_version_should_raise_error():
    with pytest.raises(SpectaclesException):
        LookerClient(
            base_url="https://spectacles.looker.com",
            client_id=os.environ.get("LOOKER_CLIENT_ID"),
            client_secret=os.environ.get("LOOKER_CLIENT_SECRET"),
            api_version=3.0,
        )


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
def test_create_query_with_dimensions_should_return_certain_fields(looker_client):
    query = looker_client.create_query(
        model="eye_exam", explore="users", dimensions=["id", "age"]
    )
    assert set(("id", "share_url")) <= set(query.keys())
    assert int(query["limit"]) == 0
    assert query["filter_expression"] == "1=2"
    assert query["model"] == "eye_exam"


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
def test_create_query_without_dimensions_should_return_certain_fields(looker_client):
    query = looker_client.create_query(model="eye_exam", explore="users", dimensions=[])
    assert set(("id", "share_url")) <= set(query.keys())
    assert int(query["limit"]) == 0
    assert query["fields"] is None
    assert query["filter_expression"] == "1=2"
    assert query["model"] == "eye_exam"


@patch("spectacles.client.requests.Session.request")
@pytest.mark.parametrize("method_name", get_client_method_names())
def test_bad_requests_should_raise_connection_errors(
    mock_request, method_name, looker_client, client_kwargs, mock_404_response
):
    """Tests each method of LookerClient for how it handles a 404 response"""
    mock_request.return_value = mock_404_response
    client_method = getattr(looker_client, method_name)
    with pytest.raises((ApiConnectionError, requests.exceptions.HTTPError)):
        client_method(**client_kwargs[method_name])


@patch("spectacles.client.requests.Session.post")
def test_authenticate_should_set_session_headers(mock_post, monkeypatch):
    mock_looker_version = Mock(spec=LookerClient.get_looker_release_version)
    mock_looker_version.return_value("1.2.3")
    monkeypatch.setattr(LookerClient, "get_looker_release_version", mock_looker_version)

    mock_post_response = Mock(spec=requests.Response)
    mock_post_response.json.return_value = {"access_token": "test_access_token"}
    mock_post.return_value = mock_post_response
    client = LookerClient("base_url", "client_id", "client_secret")
    assert client.session.headers == {"Authorization": f"token test_access_token"}
