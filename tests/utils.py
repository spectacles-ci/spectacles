from pathlib import Path
import json


def load_resource(filename):
    """Helper method to load a JSON file from tests/resources and parse it."""
    path = Path(__file__).parent / "resources" / filename
    with path.open() as file:
        return json.load(file)
