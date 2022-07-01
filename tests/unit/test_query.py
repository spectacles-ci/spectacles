import pytest
from spectacles.lookml import Dimension, Explore
from spectacles.validators.sql import Query
from copy import deepcopy


def test_query_dimensions_should_belong_to_own_explore(
    explore: Explore, dimension: Dimension
):
    # Dimensions come from different explores
    wrong_dimension = deepcopy(dimension)
    wrong_dimension.explore_name = "not_eye_exam"
    with pytest.raises(ValueError):
        Query(explore=explore, dimensions=(dimension, wrong_dimension))

    # All dimensions come from a different explore
    explore.name = "not_eye_exam"
    with pytest.raises(ValueError):
        Query(explore=explore, dimensions=(dimension, dimension))


def test_query_divide_with_different_numbers_of_dimensions(
    explore: Explore, dimension: Dimension
):
    query = Query(explore=explore, dimensions=tuple([dimension] * 2), errored=True)
    assert sorted([len(child.dimensions) for child in query.divide()]) == [1, 1]

    query = Query(explore=explore, dimensions=tuple([dimension] * 5), errored=True)
    assert sorted([len(child.dimensions) for child in query.divide()]) == [2, 3]

    query = Query(explore=explore, dimensions=tuple([dimension] * 8), errored=True)
    assert sorted([len(child.dimensions) for child in query.divide()]) == [4, 4]

    query = Query(explore=explore, dimensions=tuple([dimension] * 101), errored=True)
    assert sorted([len(child.dimensions) for child in query.divide()]) == [50, 51]


def test_query_with_one_dimension_should_not_divide(
    explore: Explore, dimension: Dimension
):
    query = Query(explore=explore, dimensions=(dimension,), errored=True)
    with pytest.raises(ValueError):
        next(query.divide())


def test_query_divide_with_chunk_size_interaction(
    explore: Explore, dimension: Dimension
):
    # Chunk size > len(dimension) / 2? Divide in half
    query = Query(
        explore=explore,
        dimensions=tuple([dimension] * 10),
        chunk_size=100,
        errored=True,
    )
    children = tuple(query.divide())
    assert len(children) == 2
    assert sorted([len(child.dimensions) for child in children]) == [5, 5]

    # Chunk size < len(dimension) / 2? Divide into N chunks
    query.chunk_size = 3
    children = tuple(query.divide())
    assert len(children) == 4
    assert sorted([len(child.dimensions) for child in children]) == [1, 3, 3, 3]

    # Chunk size == len(dimension) / 2? Divide into N chunks
    query.chunk_size = 5
    children = tuple(query.divide())
    assert len(children) == 2
    assert sorted([len(child.dimensions) for child in children]) == [5, 5]


def test_query_should_not_divide_if_not_errored(explore: Explore, dimension: Dimension):
    query = Query(explore=explore, dimensions=(dimension, dimension))
    with pytest.raises(TypeError):
        next(query.divide())

    query.errored = False
    with pytest.raises(TypeError):
        next(query.divide())

    query.errored = True
    assert next(query.divide())


def test_query_should_convert_to_profiler_format(
    explore: Explore, dimension: Dimension
):
    explore_url = "https://spectacles.looker.com/x"
    query_id = 12345
    runtime = 10.0

    query = Query(
        explore=explore,
        dimensions=(dimension, dimension),
        runtime=runtime,
        query_id=query_id,
        explore_url=explore_url,
    )
    assert query.to_profiler_format() == (explore.name, runtime, query_id, explore_url)