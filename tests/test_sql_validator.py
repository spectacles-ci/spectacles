from pathlib import Path
import json
import asyncio
from unittest.mock import patch, Mock
import pytest
import asynctest
from fonz.lookml import Project, Model, Explore, Dimension
from fonz.client import LookerClient
from fonz.validators import SqlValidator

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
            "https://test.looker.com/projects/fonz/files/test_view.view.lkml?line=340",
        ),
        Dimension(
            "test_view.dimension_two",
            "number",
            "${TABLE}.dimension_two",
            "https://test.looker.com/projects/fonz/files/test_view.view.lkml?line=360",
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


@patch("fonz.client.LookerClient.get_lookml_dimensions")
@patch("fonz.client.LookerClient.get_lookml_models")
def test_build_project(mock_get_models, mock_get_dimensions, project, validator):
    mock_get_models.return_value = load("response_models.json")
    mock_get_dimensions.return_value = load("response_dimensions.json")
    validator.build_project(selectors=["*.*"])
    assert validator.project == project
