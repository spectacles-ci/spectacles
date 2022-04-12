from copy import deepcopy
import pytest
from spectacles.lookml import Model, Explore, Dimension, build_project
from spectacles.exceptions import SpectaclesException
from utils import load_resource


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
class TestBuildProject:
    def test_model_explore_dimension_counts_should_match(self, looker_client):
        project = build_project(
            looker_client,
            name="eye_exam",
            filters=["eye_exam/users"],
            include_dimensions=True,
        )
        assert len(project.models) == 1
        assert len(project.models[0].explores) == 1
        dimensions = project.models[0].explores[0].dimensions
        assert len(dimensions) == 6
        assert "users.city" in [dim.name for dim in dimensions]
        assert not project.errored
        assert project.queried is False

    def test_project_with_everything_excluded_should_not_have_models(
        self, looker_client
    ):
        project = build_project(looker_client, name="eye_exam", filters=["-eye_exam/*"])
        assert len(project.models) == 0

    def test_duplicate_selectors_should_be_deduplicated(self, looker_client):
        project = build_project(
            looker_client, name="eye_exam", filters=["eye_exam/users", "eye_exam/users"]
        )
        assert len(project.models) == 1

    def test_hidden_dimension_should_be_excluded_with_ignore_hidden(
        self, looker_client
    ):
        project = build_project(
            looker_client,
            name="eye_exam",
            filters=["eye_exam/users"],
            include_dimensions=True,
            ignore_hidden_fields=True,
        )
        assert len(project.models[0].explores[0].dimensions) == 5


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
class TestBuildUnconfiguredProject:
    """Test for a build error when building an unconfigured LookML project."""

    def test_project_with_no_configured_models_should_raise_error(self, looker_client):
        looker_client.update_workspace(workspace="production")
        with pytest.raises(SpectaclesException):
            build_project(looker_client, name="eye_exam_unconfigured")


def test_model_from_json():
    json_dict = load_resource("response_models.json")
    model = Model.from_json(json_dict[0])
    assert model.name == "test_model_one"
    assert model.project_name == "test_project"
    assert [e.name for e in model.explores] == ["test_explore_one"]


def test_explore_from_json():
    model_name = "eye_exam"
    json_dict = load_resource("response_models.json")
    explore = Explore.from_json(json_dict[0]["explores"][0], model_name)
    assert explore.name == "test_explore_one"
    assert explore.model_name == model_name
    assert explore.dimensions == []


def test_dimension_from_json():
    model_name = "eye_exam"
    explore_name = "users"
    json_dict = load_resource("response_dimensions.json")
    dimension = Dimension.from_json(json_dict[0], model_name, explore_name)
    assert dimension.name == "test_view.dimension_one"
    assert dimension.model_name == model_name
    assert dimension.explore_name == explore_name
    assert dimension.type == "number"
    assert dimension.url == "/projects/spectacles/files/test_view.view.lkml?line=340"
    assert dimension.sql == "${TABLE}.dimension_one "
    assert not dimension.ignore


def test_ignored_dimension_with_whitespace():
    name = "test_view.dimension_one"
    model_name = "eye_exam"
    explore_name = "users"
    dimension_type = "number"
    tags = []
    url = "/projects/spectacles/files/test_view.view.lkml?line=340"
    sql = " -- spectacles: ignore\n${TABLE}.dimension_one "
    is_hidden = False
    dimension = Dimension(
        name, model_name, explore_name, dimension_type, tags, sql, url, is_hidden
    )
    assert dimension.ignore


def test_ignored_dimension_with_no_whitespace():
    name = "test_view.dimension_one"
    model_name = "eye_exam"
    explore_name = "users"
    dimension_type = "number"
    tags = []
    url = "/projects/spectacles/files/test_view.view.lkml?line=340"
    sql = "--spectacles:ignore\n${TABLE}.dimension_one "
    is_hidden = False
    dimension = Dimension(
        name, model_name, explore_name, dimension_type, tags, sql, url, is_hidden
    )
    assert dimension.ignore


def test_ignored_dimension_with_tags():
    name = "test_view.dimension_one"
    model_name = "eye_exam"
    explore_name = "users"
    dimension_type = "number"
    tags = ["spectacles: ignore"]
    url = "/projects/spectacles/files/test_view.view.lkml?line=340"
    sql = "${TABLE}.dimension_one "
    is_hidden = False
    dimension = Dimension(
        name, model_name, explore_name, dimension_type, tags, sql, url, is_hidden
    )
    assert dimension.ignore


@pytest.mark.parametrize("obj_name", ("dimension", "explore", "model", "project"))
def test_comparison_to_mismatched_type_object_should_fail(request, obj_name):
    lookml_obj = request.getfixturevalue(obj_name)

    class SomethingElse:
        ...

    assert lookml_obj != 1
    assert lookml_obj != "foo"
    assert lookml_obj != SomethingElse()


def test_assign_to_errored_should_raise_attribute_error(project):
    project.models = []
    with pytest.raises(AttributeError):
        project.errored = True


@pytest.mark.parametrize("obj_name", ("model", "project"))
def test_non_bool_errored_should_raise_value_error(request, obj_name):
    lookml_obj = request.getfixturevalue(obj_name)
    with pytest.raises(TypeError):
        lookml_obj.errored = 1


def test_dimensions_with_different_sql_can_be_equal(dimension):
    a = dimension
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


def test_comparison_to_mismatched_type_object_fails(dimension, explore, model, project):
    assert dimension != 1
    assert explore != 1
    assert model != 1
    assert project != 1


def test_explore_number_of_errors_batch_with_errors(dimension, explore, sql_error):
    explore.dimensions = [dimension]
    explore.queried = True
    explore.errors = [sql_error]
    assert explore.number_of_errors == 1


def test_explore_number_of_errors_batch_with_no_errors(dimension, explore, sql_error):
    explore.dimensions = [dimension]
    explore.queried = True
    assert explore.number_of_errors == 0


def test_explore_number_of_errors_single_with_errors(dimension, explore, sql_error):
    dimension.errors = [sql_error]
    dimension.queried = True
    explore.dimensions = [dimension, dimension]
    assert explore.number_of_errors == 2


def test_explore_number_of_errors_single_with_no_errors(dimension, explore, sql_error):
    dimension.queried = True
    explore.dimensions = [dimension, dimension]
    assert explore.number_of_errors == 0


def test_model_number_of_errors_batch_with_errors(dimension, explore, model, sql_error):
    explore.dimensions = [dimension]
    explore.queried = True
    explore.errors = [sql_error]
    model.explores = [explore, explore]
    assert model.number_of_errors == 2


def test_model_number_of_errors_batch_with_no_errors(
    dimension, explore, model, sql_error
):
    explore.dimensions = [dimension]
    explore.queried = True
    model.explores = [explore, explore]
    assert model.number_of_errors == 0


def test_model_number_of_errors_single_with_errors(
    dimension, explore, model, sql_error
):
    dimension.errors = [sql_error]
    explore.dimensions = [dimension, dimension]
    explore.queried = True
    model.explores = [explore, explore]
    assert model.number_of_errors == 4


def test_model_number_of_errors_single_with_no_errors(
    dimension, explore, model, sql_error
):
    explore.dimensions = [dimension, dimension]
    explore.queried = True
    model.explores = [explore, explore]
    assert model.number_of_errors == 0


def test_model_cannot_assign_errored_without_explorse(model):
    model.explores = []
    with pytest.raises(AttributeError):
        model.errored = True


def test_model_get_errored_explores_returns_the_correct_explore(
    model, explore, sql_error
):
    explore.queried = True
    pass_explore = deepcopy(explore)
    fail_explore = deepcopy(explore)
    fail_explore.errors = [sql_error]
    model.explores = [pass_explore, fail_explore]
    assert list(model.get_errored_explores()) == [fail_explore]


def test_project_number_of_errors_batch_with_errors(
    dimension, explore, model, project, sql_error
):
    explore.dimensions = [dimension]
    explore.queried = True
    explore.errors = [sql_error]
    model.explores = [explore, explore]
    project.models = [model]
    assert project.number_of_errors == 2


def test_project_number_of_errors_batch_with_no_errors(
    dimension, explore, model, project, sql_error
):
    explore.dimensions = [dimension]
    explore.queried = True
    model.explores = [explore, explore]
    project.models = [model]
    assert project.number_of_errors == 0


def test_project_number_of_errors_single_with_errors(
    dimension, explore, model, project, sql_error
):
    dimension.errors = [sql_error]
    explore.dimensions = [dimension, dimension]
    explore.queried = True
    model.explores = [explore, explore]
    project.models = [model, model]
    assert project.number_of_errors == 8


def test_project_number_of_errors_single_with_no_errors(
    dimension, explore, model, project, sql_error
):
    explore.dimensions = [dimension, dimension]
    explore.queried = True
    model.explores = [explore, explore]
    project.models = [model, model]
    assert project.number_of_errors == 0
