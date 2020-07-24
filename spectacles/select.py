import re
from typing import List
from spectacles.exceptions import SpectaclesException


def selector_to_pattern(selector: str) -> str:
    try:
        model, explore = selector.split("/")
        if not (model and explore):
            raise TypeError(
                "Could not extract model or explore from selector (type is None)."
            )
    except (ValueError, TypeError):
        raise SpectaclesException(
            name="invalid-selector-format",
            title="Specified explore selector is invalid.",
            detail=(
                f"'{selector}' is not a valid format. "
                "Instead, use the format 'model_name/explore_name'. "
                f"Use 'model_name/*' to select all explores in a model."
            ),
        )
    return f"^{selector.replace('*', '.+?')}$"


def is_selected(
    model: str, explore: str, selectors: List[str], exclusions: List[str]
) -> bool:
    if not selectors:
        raise ValueError("Selectors cannot be an empty list.")
    test_string = f"{model}/{explore}"
    in_any_selector = any(
        re.match(selector_to_pattern(selector), test_string) for selector in selectors
    )
    in_no_exclusions = not any(
        re.match(selector_to_pattern(exclusion), test_string)
        for exclusion in exclusions
    )
    return in_any_selector and in_no_exclusions
