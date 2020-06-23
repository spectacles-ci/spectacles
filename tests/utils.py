from typing import Dict, Any
from pathlib import Path
import json
from spectacles.types import JsonDict


def load_resource(filename) -> JsonDict:
    """Helper method to load a JSON file from tests/resources and parse it."""
    path = Path(__file__).parent / "resources" / filename
    with path.open() as file:
        return json.load(file)


def build_validation(validator) -> Dict[str, Any]:
    """Builds and returns a fake validator response object."""
    if validator == "sql":
        metadata = {"sql": "SELECT user_id FROM users"}
    elif validator == "content":
        metadata = {
            "field_name": "view_a.dimension_a",
            "content_type": "dashboard",
            "space": "Shared",
        }
    elif validator == "assert":
        metadata = {"test_name": "test_should_pass"}
    else:
        metadata = {}
    return {
        "validator": validator,
        "status": "failed",
        "tested": [
            dict(model="ecommerce", explore="orders", passed=True),
            dict(model="ecommerce", explore="sessions", passed=True),
            dict(model="ecommerce", explore="users", passed=False),
        ],
        "errors": [
            dict(
                model="ecommerce",
                explore="users",
                message="An error occurred",
                metadata=metadata,
            )
        ],
    }
