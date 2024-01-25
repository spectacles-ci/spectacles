from itertools import permutations
from typing import List

import pytest

from spectacles.exceptions import SpectaclesException
from spectacles.project_select import is_selected, selector_to_pattern


def test_invalid_format_should_raise_value_error() -> None:
    with pytest.raises(SpectaclesException):
        selector_to_pattern("model_a.explore_a")

    with pytest.raises(SpectaclesException):
        selector_to_pattern("model_a/")

    with pytest.raises(SpectaclesException):
        selector_to_pattern("explore_a")


def test_empty_selector_should_raise_value_error() -> None:
    with pytest.raises(ValueError):
        is_selected("model_a", "explore_a", [])


@pytest.mark.parametrize("filters", permutations(["model_b/explore_a", "*/*"]))
def test_select_wildcard_should_match(filters: List[str]) -> None:
    assert is_selected("model_a", "explore_a", ["*/*"])
    assert is_selected("model_a", "explore_a", filters)


def test_select_model_wildcard_should_match() -> None:
    assert is_selected("model_a", "explore_a", ["model_a/*"])
    assert is_selected("model_a", "explore_b", ["model_a/*"])


def test_select_explore_wildcard_should_match() -> None:
    assert is_selected("model_a", "explore_a", ["*/explore_a"])
    assert is_selected("model_b", "explore_a", ["*/explore_a"])


def test_select_exact_model_and_explore_should_match() -> None:
    assert is_selected("model_a", "explore_a", ["model_a/explore_a"])


def test_select_wrong_model_should_not_match() -> None:
    assert not is_selected("model_a", "explore_a", ["model_b/explore_a"])


def test_select_wrong_explore_should_not_match() -> None:
    assert not is_selected("model_a", "explore_a", ["model_a/explore_b"])


@pytest.mark.parametrize("filters", permutations(["*/*", "-*/*"]))
def test_exclude_wildcard_should_not_match(filters: List[str]) -> None:
    assert not is_selected("model_a", "explore_a", filters)


@pytest.mark.parametrize("filters", permutations(["*/*", "-model_a/*"]))
def test_exclude_model_wildcard_should_not_match(filters: List[str]) -> None:
    assert not is_selected("model_a", "explore_a", filters)


@pytest.mark.parametrize("filters", permutations(["*/*", "-*/explore_a"]))
def test_exclude_explore_wildcard_should_not_match(filters: List[str]) -> None:
    assert not is_selected("model_a", "explore_a", filters)


@pytest.mark.parametrize("filters", permutations(["*/*", "-model_a/explore_a"]))
def test_exclude_exact_model_and_explore_should_not_match(filters: List[str]) -> None:
    assert not is_selected("model_a", "explore_a", filters)
