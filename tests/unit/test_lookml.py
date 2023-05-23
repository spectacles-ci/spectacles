from copy import deepcopy
import pytest
from spectacles.lookml import Model, Explore, LookMlField, Project
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
    assert explore.fields == []


def test_field_from_json():
    model_name = "eye_exam"
    explore_name = "users"
    json_dict = load_resource("response_fields.json")
    field = LookMlField.from_json(json_dict[0], model_name, explore_name)
    assert field.name == "test_view.dimension_one"
    assert field.model_name == model_name
    assert field.explore_name == explore_name
    assert field.type == "number"
    assert field.url == "/projects/spectacles/files/test_view.view.lkml?line=340"
    assert field.sql == "${TABLE}.dimension_one "
    assert not field.ignore


def test_ignored_field_with_whitespace():
    name = "test_view.dimension_one"
    model_name = "eye_exam"
    explore_name = "users"
    field_type = "number"
    tags = []
    url = "/projects/spectacles/files/test_view.view.lkml?line=340"
    sql = " -- spectacles: ignore\n${TABLE}.dimension_one "
    is_hidden = False
    field = LookMlField(
        name, model_name, explore_name, field_type, tags, sql, url, is_hidden
    )
    assert field.ignore


def test_ignored_field_with_no_whitespace():
    name = "test_view.dimension_one"
    model_name = "eye_exam"
    explore_name = "users"
    field_type = "number"
    tags = []
    url = "/projects/spectacles/files/test_view.view.lkml?line=340"
    sql = "--spectacles:ignore\n${TABLE}.dimension_one "
    is_hidden = False
    field = LookMlField(
        name, model_name, explore_name, field_type, tags, sql, url, is_hidden
    )
    assert field.ignore


def test_ignored_field_with_tags():
    name = "test_view.dimension_one"
    model_name = "eye_exam"
    explore_name = "users"
    field_type = "number"
    tags = ["spectacles: ignore"]
    url = "/projects/spectacles/files/test_view.view.lkml?line=340"
    sql = "${TABLE}.dimension_one "
    is_hidden = False
    field = LookMlField(
        name, model_name, explore_name, field_type, tags, sql, url, is_hidden
    )
    assert field.ignore


@pytest.mark.parametrize("obj_name", ("field", "explore", "model", "project"))
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
        project.errored = True


@pytest.mark.parametrize("obj_name", ("model", "project"))
def test_non_bool_errored_should_raise_value_error(
    request: pytest.FixtureRequest, obj_name: str
):
    lookml_obj = request.getfixturevalue(obj_name)
    with pytest.raises(TypeError):
        lookml_obj.errored = 1


def test_fields_with_different_sql_can_be_equal(field: LookMlField):
    a = field
    b = deepcopy(a)
    b.sql = "${TABLE}.another_column"
    assert a == b


def test_field_should_not_be_errored_if_not_queried(
    field: LookMlField, sql_error: SqlError
):
    assert field.errored is None
    field.errors = [sql_error]
    assert field.errored is None
    field.queried = True
    assert field.errored is True


def test_should_not_be_able_to_set_errored_on_field(field: LookMlField):
    with pytest.raises(AttributeError):
        field.errored = True


def test_should_not_be_able_to_set_errored_on_explore(explore: Explore):
    with pytest.raises(AttributeError):
        explore.errored = True


def test_fields_can_be_sorted_by_name():
    unsorted = [
        LookMlField(
            name="b",
            model_name="",
            explore_name="",
            type="",
            tags=[],
            sql="",
            is_hidden=False,
        ),
        LookMlField(
            name="a",
            model_name="",
            explore_name="",
            type="",
            tags=[],
            sql="",
            is_hidden=False,
        ),
        LookMlField(
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
    assert [field.name for field in sorted(unsorted)] == ["a", "b", "c"]


def test_fields_can_be_sorted_by_explore_name():
    unsorted = [
        LookMlField(
            name="",
            model_name="",
            explore_name="b",
            type="",
            tags=[],
            sql="",
            is_hidden=False,
        ),
        LookMlField(
            name="",
            model_name="",
            explore_name="c",
            type="",
            tags=[],
            sql="",
            is_hidden=False,
        ),
        LookMlField(
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
    assert [field.explore_name for field in sorted(unsorted)] == ["a", "b", "c"]


def test_parent_queried_behavior_should_depend_on_its_child(
    explore: Explore, field: LookMlField, model, project: Project
):
    for parent, child, attr in [
        (explore, field, "fields"),
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


def test_comparison_to_mismatched_type_object_fails(
    field: LookMlField, explore: Explore, model, project: Project
):
    assert field != 1
    assert explore != 1
    assert model != 1
    assert project != 1


def test_explore_number_of_errors_batch_with_errors(
    field: LookMlField, explore: Explore, sql_error: SqlError
):
    explore.fields = [field]
    explore.queried = True
    explore.errors = [sql_error]
    assert explore.number_of_errors == 1


def test_explore_number_of_errors_batch_with_no_errors(
    field: LookMlField, explore: Explore
):
    explore.fields = [field]
    explore.queried = True
    assert explore.number_of_errors == 0


def test_explore_number_of_errors_single_with_errors(
    field: LookMlField, explore: Explore, sql_error: SqlError
):
    field.errors = [sql_error]
    field.queried = True
    explore.fields = [field, field]
    assert explore.number_of_errors == 2


def test_explore_number_of_errors_single_with_no_errors(
    field: LookMlField, explore: Explore
):
    field.queried = True
    explore.fields = [field, field]
    assert explore.number_of_errors == 0


def test_model_number_of_errors_batch_with_errors(
    field: LookMlField, explore: Explore, model: Model, sql_error: SqlError
):
    explore.fields = [field]
    explore.queried = True
    explore.errors = [sql_error]
    model.explores = [explore, explore]
    assert model.number_of_errors == 2


def test_model_number_of_errors_batch_with_no_errors(
    field: LookMlField, explore: Explore, model: Model
):
    explore.fields = [field]
    explore.queried = True
    model.explores = [explore, explore]
    assert model.number_of_errors == 0


def test_model_number_of_errors_single_with_errors(
    field: LookMlField, explore: Explore, model: Model, sql_error: SqlError
):
    field.errors = [sql_error]
    explore.fields = [field, field]
    explore.queried = True
    model.explores = [explore, explore]
    assert model.number_of_errors == 4


def test_model_number_of_errors_single_with_no_errors(
    field: LookMlField, explore: Explore, model: Model
):
    explore.fields = [field, field]
    explore.queried = True
    model.explores = [explore, explore]
    assert model.number_of_errors == 0


def test_model_cannot_assign_errored_without_explorse(model: Model):
    model.explores = []
    with pytest.raises(AttributeError):
        model.errored = True


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
    field: LookMlField,
    explore: Explore,
    model: Model,
    project: Project,
    sql_error: SqlError,
):
    explore.fields = [field]
    explore.queried = True
    explore.errors = [sql_error]
    model.explores = [explore, explore]
    project.models = [model]
    assert project.number_of_errors == 2


def test_project_number_of_errors_batch_with_no_errors(
    field: LookMlField, explore: Explore, model: Model, project: Project
):
    explore.fields = [field]
    explore.queried = True
    model.explores = [explore, explore]
    project.models = [model]
    assert project.number_of_errors == 0


def test_project_number_of_errors_single_with_errors(
    field: LookMlField,
    explore: Explore,
    model: Model,
    project: Project,
    sql_error: SqlError,
):
    field.errors = [sql_error]
    explore.fields = [field, field]
    explore.queried = True
    model.explores = [explore, explore]
    project.models = [model, model]
    assert project.number_of_errors == 8


def test_project_number_of_errors_single_with_no_errors(
    field: LookMlField, explore: Explore, model: Model, project: Project
):
    explore.fields = [field, field]
    explore.queried = True
    model.explores = [explore, explore]
    project.models = [model, model]
    assert project.number_of_errors == 0
