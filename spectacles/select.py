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


def is_selected(model: str, explore: str, filters: List[str]) -> bool:
    if not filters:
        raise ValueError("Filters cannot be an empty list.")

    test_string = f"{model}/{explore}"
    included = True
    for f in filters:
        # If it matches an exclude, stop immediately
        if f[0] == "~":
            if re.match(selector_to_pattern(f[1:]), test_string):
                return False
        else:
            if re.match(selector_to_pattern(f), test_string):
                included = True
            # If we encounter a non-matching include, assume we're not included unless
            # we find a matching include later in `filters`
            else:
                included = False

    return included
