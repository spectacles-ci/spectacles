import pytest
from spectacles.client import LookerClient
from spectacles.validators.content import ContentValidator


@pytest.fixture
def validator(looker_client: LookerClient) -> ContentValidator:
    return ContentValidator(looker_client, exclude_personal=True)


def test_get_content_type_with_bad_keys_should_raise_key_error(
    validator: ContentValidator,
):
    content = {"tableau_dashboard": "Something goes here."}
    with pytest.raises(KeyError):
        validator._get_content_type(content)


def test_get_tile_type_with_bad_keys_should_raise_key_error(
    validator: ContentValidator,
):
    content = {"lookml_dashboard": "Something goes here."}
    with pytest.raises(KeyError):
        validator._get_tile_type(content)
