from typing import List, Callable, Tuple
from unittest.mock import Mock, patch
import pytest
import requests
import inspect
from spectacles.client import LookerClient
from spectacles.exceptions import SpectaclesException, ApiConnectionError

TEST_BASE_URL = "https://test.looker.com"
TEST_CLIENT_ID = "test_client_id"
TEST_CLIENT_SECRET = "test_client_secret"


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
def client_kwargs():
    return dict(
        authenticate={
            "client_id": TEST_CLIENT_ID,
            "client_secret": TEST_CLIENT_SECRET,
            "api_version": 3.1,
        },
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


@pytest.fixture
def client(monkeypatch):
    mock_authenticate = Mock(spec=LookerClient.authenticate)
    monkeypatch.setattr(LookerClient, "authenticate", mock_authenticate)
    return LookerClient(TEST_BASE_URL, TEST_CLIENT_ID, TEST_CLIENT_SECRET)


@pytest.fixture
def mock_404_response():
    mock = Mock(spec=requests.Response)
    mock.status_code = 404
    mock.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "An HTTP error occurred."
    )
    return mock


@patch("spectacles.client.requests.Session.request")
@pytest.mark.parametrize("method_name", get_client_method_names())
def test_bad_request_raises_connection_error(
    mock_request, method_name, client, client_kwargs, mock_404_response
):
    """Tests each method of LookerClient for how it handles a 404 response"""
    mock_request.return_value = mock_404_response
    client_method = getattr(client, method_name)
    with pytest.raises((ApiConnectionError, requests.exceptions.HTTPError)):
        client_method(**client_kwargs[method_name])


@patch("spectacles.client.LookerClient.authenticate")
def test_unsupported_api_version_raises_error(mock_authenticate):
    with pytest.raises(SpectaclesException):
        LookerClient(
            base_url=TEST_BASE_URL,
            client_id=TEST_CLIENT_ID,
            client_secret=TEST_CLIENT_SECRET,
            api_version=3.0,
        )


@patch("spectacles.client.requests.Session.post")
def test_authenticate_sets_session_headers(mock_post, monkeypatch):
    mock_looker_version = Mock(spec=LookerClient.get_looker_release_version)
    mock_looker_version.return_value("1.2.3")
    monkeypatch.setattr(LookerClient, "get_looker_release_version", mock_looker_version)

    mock_post_response = Mock(spec=requests.Response)
    mock_post_response.json.return_value = {"access_token": "test_access_token"}
    mock_post.return_value = mock_post_response
    client = LookerClient(TEST_BASE_URL, TEST_CLIENT_ID, TEST_CLIENT_SECRET)
    assert client.session.headers == {"Authorization": f"token test_access_token"}


@patch("spectacles.client.requests.Session.get")
def test_get_looker_release_version(mock_get, client):
    mock_get.return_value.json.return_value = {"looker_release_version": "6.24.12"}
    version = client.get_looker_release_version()
    assert version == "6.24.12"


@patch("spectacles.client.requests.Session.get")
def test_get_looker_release_version(mock_get, client):
    mock_get.return_value.json.return_value = {
        "looker_release_version": "6.24.12",
        "current_version": "6.24.12",
        "supported_version": ["6.24.10", "6.24.10"],
    }
    version = client.get_looker_release_version()
    assert version == "6.24.12"


@patch("spectacles.client.requests.Session.post")
def test_create_query(mock_post, client):
    QUERY_ID = 124950204921
    mock_post.return_value.json.return_value = {"id": QUERY_ID}
    query_id = client.create_query(
        "test_model", "test_explore_one", ["dimension_one", "dimension_two"]
    )
    assert query_id == QUERY_ID
    mock_post.assert_called_once_with(
        url="https://test.looker.com:19999/api/3.1/queries",
        timeout=300,
        json={
            "model": "test_model",
            "view": "test_explore_one",
            "fields": ["dimension_one", "dimension_two"],
            "limit": 0,
            "filter_expression": "1=2",
        },
    )
