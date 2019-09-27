from pathlib import Path
import json
import pytest
from spectacles.lookml import Project, Model, Explore, Dimension


@pytest.fixture
def dimension():
    return Dimension(
        name="dimension_name",
        type="string",
        sql="${TABLE}.dimension_name",
        url="https://test.looker.com",
    )


@pytest.fixture
def explore():
    return Explore(name="explore_name")


@pytest.fixture
def model():
    return Model(name="model_name", project="string", explores=[])


@pytest.fixture
def project():
    return Project(name="project_name", models=[])


def load(filename):
    """Helper method to load a JSON file from tests/resources and parse it."""
    path = Path(__file__).parent / "resources" / filename
    with path.open() as file:
        return json.load(file)


def test_model_from_json():
    json_dict = load("response_models.json")
    model = Model.from_json(json_dict[0])
    assert model.name == "test_model_one"
    assert model.project == "test_project"
    assert [e.name for e in model.explores] == ["test_explore_one"]


def test_explore_from_json():
    json_dict = load("response_models.json")
    explore = Explore.from_json(json_dict[0]["explores"][0])
    assert explore.name == "test_explore_one"
    assert explore.dimensions == []


def test_dimension_from_json():
    json_dict = load("response_dimensions.json")
    dimension = Dimension.from_json(json_dict[0])
    assert dimension.name == "test_view.dimension_one"
    assert dimension.type == "number"
    assert dimension.url == "/projects/spectacles/files/test_view.view.lkml?line=340"
    assert dimension.sql == "${TABLE}.dimension_one "
    assert not dimension.ignore


def test_ignored_dimension_with_whitespace():
    name = "test_view.dimension_one"
    dimension_type = "number"
    url = "/projects/spectacles/files/test_view.view.lkml?line=340"
    sql = " -- spectacles: ignore\n${TABLE}.dimension_one "
    dimension = Dimension(name, dimension_type, sql, url)
    assert dimension.ignore


def test_ignored_dimension_with_no_whitespace():
    name = "test_view.dimension_one"
    dimension_type = "number"
    url = "/projects/spectacles/files/test_view.view.lkml?line=340"
    sql = "--spectacles:ignore\n${TABLE}.dimension_one "
    dimension = Dimension(name, dimension_type, sql, url)
    assert dimension.ignore


def test_comparison_to_mismatched_type_object_fails(dimension, explore, model, project):
    assert dimension != 1
    assert explore != 1
    assert model != 1
    assert project != 1
