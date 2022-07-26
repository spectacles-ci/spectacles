from copy import deepcopy
import pytest
from spectacles.lookml import Model, Explore, Dimension, Project
from spectacles.exceptions import SqlError
from tests.utils import load_resource


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
def test_comparison_to_mismatched_type_object_should_fail(
    request: pytest.FixtureRequest, obj_name: str
):
    lookml_obj = request.getfixturevalue(obj_name)

    class SomethingElse:
        ...

    assert lookml_obj != 1
    assert lookml_obj != "foo"
    assert lookml_obj != SomethingElse()


def test_assign_to_errored_should_raise_attribute_error(project: Project):
    project.models = []
    with pytest.raises(AttributeError):
        project.errored = True  # type: ignore


def test_dimensions_with_different_sql_can_be_equal(dimension: Dimension):
    a = dimension
    b = deepcopy(a)
    b.sql = "${TABLE}.another_column"
    assert a == b


def test_dimension_should_not_be_errored_if_not_queried(
    dimension: Dimension, sql_error: SqlError
):
    assert dimension.errored is None
    dimension.errors = [sql_error]
    assert dimension.errored is None
    dimension.queried = True
    assert dimension.errored is True


def test_should_not_be_able_to_set_errored_on_dimension(dimension: Dimension):
    with pytest.raises(AttributeError):
        dimension.errored = True


def test_should_not_be_able_to_set_errored_on_explore(explore: Explore):
    with pytest.raises(AttributeError):
        explore.errored = True


def test_dimensions_can_be_sorted_by_name():
    unsorted = [
        Dimension(
            name="b",
            model_name="",
            explore_name="",
            type="",
            tags=[],
            sql="",
            is_hidden=False,
        ),
        Dimension(
            name="a",
            model_name="",
            explore_name="",
            type="",
            tags=[],
            sql="",
            is_hidden=False,
        ),
        Dimension(
            name="c",
            model_name="",
            explore_name="",
            type="",
            tags=[],
            sql="",
            is_hidden=False,
        ),
    ]

    assert sorted(unsorted) != unsorted
    assert [dimension.name for dimension in sorted(unsorted)] == ["a", "b", "c"]


def test_dimensions_can_be_sorted_by_explore_name():
    unsorted = [
        Dimension(
            name="",
            model_name="",
            explore_name="b",
            type="",
            tags=[],
            sql="",
            is_hidden=False,
        ),
        Dimension(
            name="",
            model_name="",
            explore_name="c",
            type="",
            tags=[],
            sql="",
            is_hidden=False,
        ),
        Dimension(
            name="",
            model_name="",
            explore_name="a",
            type="",
            tags=[],
            sql="",
            is_hidden=False,
        ),
    ]

    assert sorted(unsorted) != unsorted
    assert [dimension.explore_name for dimension in sorted(unsorted)] == ["a", "b", "c"]


def test_comparison_to_mismatched_type_object_fails(
    dimension: Dimension, explore: Explore, model, project: Project
):
    assert dimension != 1
    assert explore != 1
    assert model != 1
    assert project != 1


def test_explore_number_of_errors_batch_with_errors(
    dimension: Dimension, explore: Explore, sql_error: SqlError
):
    explore.dimensions = [dimension]
    explore.queried = True
    explore.errors = [sql_error]
    assert explore.number_of_errors == 1


def test_explore_number_of_errors_batch_with_no_errors(
    dimension: Dimension, explore: Explore
):
    explore.dimensions = [dimension]
    explore.queried = True
    assert explore.number_of_errors == 0


def test_explore_number_of_errors_single_with_errors(
    dimension: Dimension, explore: Explore, sql_error: SqlError
):
    dimension.errors = [sql_error]
    dimension.queried = True
    explore.dimensions = [dimension, dimension]
    assert explore.number_of_errors == 2


def test_explore_number_of_errors_single_with_no_errors(
    dimension: Dimension, explore: Explore
):
    dimension.queried = True
    explore.dimensions = [dimension, dimension]
    assert explore.number_of_errors == 0


def test_model_number_of_errors_batch_with_errors(
    dimension: Dimension, explore: Explore, model: Model, sql_error: SqlError
):
    explore.dimensions = [dimension]
    explore.queried = True
    explore.errors = [sql_error]
    model.explores = [explore, explore]
    assert model.number_of_errors == 2


def test_model_number_of_errors_batch_with_no_errors(
    dimension: Dimension, explore: Explore, model: Model
):
    explore.dimensions = [dimension]
    explore.queried = True
    model.explores = [explore, explore]
    assert model.number_of_errors == 0


def test_model_number_of_errors_single_with_errors(
    dimension: Dimension, explore: Explore, model: Model, sql_error: SqlError
):
    dimension.errors = [sql_error]
    dimension.queried = True
    explore.dimensions = [dimension, dimension]
    explore.queried = True
    model.explores = [explore, explore]
    assert model.number_of_errors == 4


def test_model_number_of_errors_single_with_no_errors(
    dimension: Dimension, explore: Explore, model: Model
):
    explore.dimensions = [dimension, dimension]
    explore.queried = True
    model.explores = [explore, explore]
    assert model.number_of_errors == 0


def test_model_cannot_assign_errored_without_explorse(model: Model):
    model.explores = []
    with pytest.raises(AttributeError):
        model.errored = True  # type: ignore


def test_model_get_errored_explores_returns_the_correct_explore(
    model: Model, explore: Explore, sql_error: SqlError
):
    explore.queried = True
    pass_explore = deepcopy(explore)
    fail_explore = deepcopy(explore)
    fail_explore.errors = [sql_error]
    model.explores = [pass_explore, fail_explore]
    assert list(model.get_errored_explores()) == [fail_explore]


def test_project_number_of_errors_batch_with_errors(
    dimension: Dimension,
    explore: Explore,
    model: Model,
    project: Project,
    sql_error: SqlError,
):
    explore.dimensions = [dimension]
    explore.queried = True
    explore.errors = [sql_error]
    model.explores = [explore, explore]
    project.models = [model]
    assert project.number_of_errors == 2


def test_project_number_of_errors_batch_with_no_errors(
    dimension: Dimension, explore: Explore, model: Model, project: Project
):
    explore.dimensions = [dimension]
    explore.queried = True
    model.explores = [explore, explore]
    project.models = [model]
    assert project.number_of_errors == 0


def test_project_number_of_errors_single_with_errors(
    dimension: Dimension,
    explore: Explore,
    model: Model,
    project: Project,
    sql_error: SqlError,
):
    dimension.errors = [sql_error]
    dimension.queried = True
    explore.dimensions = [dimension, dimension]
    explore.queried = True
    model.explores = [explore, explore]
    project.models = [model, model]
    assert project.number_of_errors == 8


def test_project_number_of_errors_single_with_no_errors(
    dimension: Dimension, explore: Explore, model: Model, project: Project
):
    explore.dimensions = [dimension, dimension]
    explore.queried = True
    model.explores = [explore, explore]
    project.models = [model, model]
    assert project.number_of_errors == 0
