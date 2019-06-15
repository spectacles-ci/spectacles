from pathlib import Path
import json
from fonz.lookml import Model, Explore, Dimension


def load(filename):
    """Helper method to load a JSON file from tests/resources and parse it."""
    path = Path(__file__).parent / "resources" / filename
    with path.open() as file:
        return json.load(file)


def test_model_from_json():
    json_dict = load("response_models.json")
    model = Model.from_json(json_dict[0])
    assert model.name == "fonz"
    assert model.project == "fonz"
    assert [e.name for e in model.explores] == ["test_explore"]


def test_explore_from_json():
    json_dict = load("response_models.json")
    explore = Explore.from_json(json_dict[0]["explores"][0])
    assert explore.name == "test_explore"
    assert explore.dimensions == []


def test_dimension_from_json():
    json_dict = load("response_dimensions.json")
    dimension = Dimension.from_json(json_dict[0])
    assert dimension.name == "test_view.aHivKlGZVzjK"
    assert dimension.type == "number"
    assert dimension.url == "/projects/fonz/files/test_view.view.lkml?line=340"
    assert dimension.sql == "${TABLE}.annual_rent "
    assert dimension.ignore == False
