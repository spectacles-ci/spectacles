import pytest
import respx

from spectacles.client import LookerClient
from spectacles.lookml import Project
from spectacles.validators.content import ContentValidator


@pytest.fixture
def validator(looker_client: LookerClient) -> ContentValidator:
    return ContentValidator(looker_client, exclude_personal=True, folders=["1", "-2"])


def test_get_content_type_with_bad_keys_should_raise_key_error(
    validator: ContentValidator,
) -> None:
    content = {"tableau_dashboard": "Something goes here."}
    with pytest.raises(KeyError):
        validator._get_content_type(content)


def test_get_tile_type_with_bad_keys_should_raise_key_error(
    validator: ContentValidator,
) -> None:
    content = {"lookml_dashboard": "Something goes here."}
    with pytest.raises(KeyError):
        validator._get_tile_type(content)


@pytest.mark.asyncio
async def test_validate_should_pass_project_and_folders_to_client(
    validator: ContentValidator,
    project: Project,
    mocked_api: respx.MockRouter,
) -> None:
    mocked_api.get("folders", name="all_folders").respond(
        200,
        json=[
            {
                "id": "1",
                "is_personal": False,
                "is_personal_descendant": False,
                "parent_id": None,
            },
            {
                "id": "2",
                "is_personal": False,
                "is_personal_descendant": False,
                "parent_id": "1",
            },
        ],
    )
    mocked_api.get(
        "content_validation",
        params={"project_names": [project.name], "space_ids": ["1"]},
        name="content_validation",
    ).respond(200, json={"content_with_errors": []})
    await validator.validate(project)
    mocked_api["content_validation"].calls.assert_called_once()
