import pytest

from unittest.mock import AsyncMock

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
    looker_client: LookerClient,
) -> None:
    looker_client.content_validation = AsyncMock(
        return_value={"content_with_errors": []}
    )
    looker_client.all_folders = AsyncMock(
        return_value=[
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
        ]
    )
    await validator.validate(project)
    looker_client.content_validation.assert_called_once_with(
        project_names=[project.name], space_ids=["1"]
    )
