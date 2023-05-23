import pytest
from spectacles.lookml import LookMlField, Explore
from spectacles.validators.sql import Query
from copy import deepcopy


def test_query_fields_should_belong_to_own_explore(
    explore: Explore, field: LookMlField
):
    # Fields come from different explores
    wrong_field = deepcopy(field)
    wrong_field.explore_name = "not_eye_exam"
    with pytest.raises(ValueError):
        Query(explore=explore, fields=(field, wrong_field))

    # All fields come from a different explore
    explore.name = "not_eye_exam"
    with pytest.raises(ValueError):
        Query(explore=explore, fields=(field, field))


def test_query_divide_with_different_numbers_of_fields(
    explore: Explore, field: LookMlField
):
    query = Query(explore=explore, fields=tuple([field] * 2), errored=True)
    assert sorted([len(child.fields) for child in query.divide()]) == [1, 1]

    query = Query(explore=explore, fields=tuple([field] * 5), errored=True)
    assert sorted([len(child.fields) for child in query.divide()]) == [2, 3]

    query = Query(explore=explore, fields=tuple([field] * 8), errored=True)
    assert sorted([len(child.fields) for child in query.divide()]) == [4, 4]

    query = Query(explore=explore, fields=tuple([field] * 101), errored=True)
    assert sorted([len(child.fields) for child in query.divide()]) == [50, 51]


def test_query_with_one_field_should_not_divide(explore: Explore, field: LookMlField):
    query = Query(explore=explore, fields=(field,), errored=True)
    with pytest.raises(ValueError):
        next(query.divide())


def test_query_should_not_divide_if_not_errored(explore: Explore, field: LookMlField):
    query = Query(explore=explore, fields=(field, field))
    with pytest.raises(TypeError):
        next(query.divide())

    query.errored = False
    with pytest.raises(TypeError):
        next(query.divide())

    query.errored = True
    assert next(query.divide())


def test_query_should_convert_to_profiler_format(explore: Explore, field: LookMlField):
    explore_url = "https://spectacles.looker.com/x"
    query_id = "12345"
    runtime = 10.0

    query = Query(
        explore=explore,
        fields=(field, field),
        runtime=runtime,
        query_id=query_id,
        explore_url=explore_url,
    )
    assert query.to_profiler_format() == (
        explore.name,
        "*",
        runtime,
        query_id,
        explore_url,
    )

    query = Query(
        explore=explore,
        fields=(field,),
        runtime=runtime,
        query_id=query_id,
        explore_url=explore_url,
    )
    assert query.to_profiler_format() == (
        explore.name,
        field.name,
        runtime,
        query_id,
        explore_url,
    )
