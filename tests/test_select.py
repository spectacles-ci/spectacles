import pytest
from spectacles.select import is_selected, selector_to_pattern
from spectacles.exceptions import SpectaclesException


def test_invalid_format_should_raise_value_error():
    with pytest.raises(SpectaclesException):
        selector_to_pattern("model_a.explore_a")

    with pytest.raises(SpectaclesException):
        selector_to_pattern("model_a/")

    with pytest.raises(SpectaclesException):
        selector_to_pattern("explore_a")


def test_empty_selector_should_raise_value_error():
    with pytest.raises(ValueError):
        is_selected("model_a", "explore_a", [], [])


def test_select_wildcard_should_match():
    assert is_selected("model_a", "explore_a", ["*/*"], [])
    assert is_selected("model_a", "explore_a", ["model_b/explore_a", "*/*"], [])


def test_select_model_wildcard_should_match():
    assert is_selected("model_a", "explore_a", ["model_a/*"], [])
    assert is_selected("model_a", "explore_b", ["model_a/*"], [])


def test_select_explore_wildcard_should_match():
    assert is_selected("model_a", "explore_a", ["*/explore_a"], [])
    assert is_selected("model_b", "explore_a", ["*/explore_a"], [])


def test_select_exact_model_and_explore_should_match():
    assert is_selected("model_a", "explore_a", ["model_a/explore_a"], [])


def test_select_wrong_model_should_not_match():
    assert not is_selected("model_a", "explore_a", ["model_b/explore_a"], [])


def test_select_wrong_explore_should_not_match():
    assert not is_selected("model_a", "explore_a", ["model_a/explore_b"], [])


def test_exclude_wildcard_should_not_match():
    assert not is_selected("model_a", "explore_a", ["*/*"], ["*/*"])


def test_exclude_model_wildcard_should_not_match():
    assert not is_selected("model_a", "explore_a", ["*/*"], ["model_a/*"])


def test_exclude_explore_wildcard_should_not_match():
    assert not is_selected("model_a", "explore_a", ["*/*"], ["*/explore_a"])


def test_exclude_exact_model_and_explore_should_not_match():
    assert not is_selected("model_a", "explore_a", ["*/*"], ["model_a/explore_a"])
