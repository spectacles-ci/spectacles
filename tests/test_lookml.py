from pathlib import Path
from copy import deepcopy
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
    return Model(name="model_name", project="project_name", explores=[])


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


@pytest.mark.parametrize("obj_name", ("dimension", "explore", "model", "project"))
def test_comparison_to_mismatched_type_object_should_fail(request, obj_name):
    lookml_obj = request.getfixturevalue(obj_name)

    class SomethingElse:
        ...

    assert lookml_obj != 1
    assert lookml_obj != "foo"
    assert lookml_obj != SomethingElse()


def test_dimensions_with_different_sql_can_be_equal():
    a = Dimension(
        name="a",
        type="string",
        sql="${TABLE}.some_column",
        url="https://test.looker.com",
    )
    b = deepcopy(a)
    b.sql = "${TABLE}.another_column"
    assert a == b


def test_dimension_should_not_be_errored_if_not_queried(dimension, sql_error):
    assert dimension.errored is None
    dimension.errors = [sql_error]
    assert dimension.errored is None
    dimension.queried = True
    assert dimension.errored is True


def test_should_not_be_able_to_set_errored_on_dimension(dimension):
    with pytest.raises(AttributeError):
        dimension.errored = True


def test_should_not_be_able_to_set_errored_on_explore(explore):
    with pytest.raises(AttributeError):
        explore.errored = True


def test_parent_queried_behavior_should_depend_on_its_child(
    explore, dimension, model, project
):
    for parent, child, attr in [
        (explore, dimension, "dimensions"),
        (model, explore, "explores"),
        (project, model, "models"),
    ]:
        child.queried = False
        parent.queried = False
        a = child
        b = deepcopy(child)
        children = getattr(parent, attr)
        children.append(a)
        assert parent.queried is False
        a.queried = True
        assert parent.queried is True
        children.append(b)
        assert parent.queried is True
