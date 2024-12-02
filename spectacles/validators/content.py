from typing import Any, Dict, List, Optional

from spectacles.client import LookerClient
from spectacles.exceptions import ContentError, SpectaclesException
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.lookml import Explore, Model, Project
from spectacles.models import JsonDict


class ContentValidator:
    def __init__(
        self,
        client: LookerClient,
        exclude_personal: bool = False,
        folders: Optional[List[str]] = None,
    ):
        self.client = client
        self.exclude_personal = exclude_personal
        self.include_folders: List[str] = []
        self.exclude_folders: List[str] = []

        if folders:
            for folder_id in folders:
                if folder_id.startswith("-"):
                    self.exclude_folders.append(folder_id[1:])
                else:
                    self.include_folders.append(folder_id)

    async def validate(self, project: Project) -> List[ContentError]:
        personal_folders = (
            await self._get_personal_folders() if self.exclude_personal else []
        )

        self.excluded_folders: List[str] = personal_folders + (
            await self._get_all_subfolders(self.exclude_folders)
            if self.exclude_folders
            else []
        )
        self.included_folders: List[str] = (
            await self._get_all_subfolders(self.include_folders)
            if self.include_folders
            else []
        )

        def is_folder_selected(folder_id: Optional[str]) -> bool:
            if folder_id in self.excluded_folders:
                return False
            if self.included_folders and folder_id not in self.included_folders:
                return False
            else:
                return True

        result = await self.client.content_validation()
        project.queried = True

        content_errors: List[ContentError] = []
        for content in result["content_with_errors"]:
            # Skip content dicts if they lack a `look` or `dashboard` key
            try:
                content_type = self._get_content_type(content)
            except KeyError:
                logger.warning(
                    "Warning: Skipping some content because it does not seem to be a "
                    "Dashboard or a Look."
                )
                logger.debug(f"The unidentified content received was: {content}")
                continue

            # Sometimes the content no longer exists, in which case the folder is None
            folder = content[content_type].get("folder")
            folder_id: Optional[str] = folder.get("id") if folder else None
            # If exclude_personal isn't specified, personal_folders list is empty
            if not is_folder_selected(folder_id):
                continue
            else:
                errors = self._get_errors_from_result(project, content, content_type)
                content_errors.extend(errors)

        return content_errors

    async def _get_personal_folders(self) -> List[str]:
        personal_folders = []
        result = await self.client.all_folders()
        for folder in result:
            if folder["is_personal"] or folder["is_personal_descendant"]:
                personal_folders.append(folder["id"])
        return personal_folders

    async def _get_all_subfolders(self, input_folders: List[str]) -> List[str]:
        result = []
        all_folders = await self.client.all_folders()
        for folder_id in input_folders:
            if not any(folder["id"] == folder_id for folder in all_folders):
                raise SpectaclesException(
                    name="folder-id-input-does-not-exist",
                    title="One of the folders input doesn't exist.",
                    detail=f"Folder {folder_id} is not a valid folder number.",
                )
            result.extend(self._get_subfolders(folder_id, all_folders))
        return result

    def _get_subfolders(self, folder_id: str, all_folders: List[JsonDict]) -> List[str]:
        subfolders = [folder_id]
        children = [
            child["id"] for child in all_folders if child["parent_id"] == folder_id
        ]
        if children:
            for child in children:
                subfolders.extend(self._get_subfolders(child, all_folders))
        return subfolders

    @staticmethod
    def _get_content_type(content: Dict[str, Any]) -> str:
        CONTENT_TYPES = ("look", "dashboard")
        for content_type in CONTENT_TYPES:
            if content.get(content_type):
                return content_type

        # If none of the content types are found
        raise KeyError(
            f"Content type not found. Valid keys are: {', '.join(CONTENT_TYPES)}"
        )

    @staticmethod
    def _get_tile_type(content: Dict[str, Any]) -> str:
        if content["dashboard_element"]:
            return "dashboard_element"
        elif content["dashboard_filter"]:
            return "dashboard_filter"
        else:
            raise KeyError(
                "Tile type not found. Valid keys are 'dashboard_element', 'dashboard_filter'"
            )

    def _get_errors_from_result(
        self, project: Project, result: Dict[str, Any], content_type: str
    ) -> List[ContentError]:
        content_errors: List[ContentError] = []
        for error in result["errors"]:
            model_name = error["model_name"]
            explore_name = error["explore_name"]
            model: Optional[Model] = project.get_model(model_name)
            if model:
                explore: Optional[Explore] = model.get_explore(name=explore_name)
            else:
                explore = None
            # Skip errors that are not associated with selected explores or existing models
            if explore or model:
                content_id = result[content_type]["id"]
                folder = result[content_type].get("folder")
                folder_name: Optional[str] = folder.get("name") if folder else None
                content_error = ContentError(
                    model=model_name,
                    explore=explore_name,
                    message=error["message"],
                    field_name=error["field_name"],
                    content_type=content_type,
                    title=result[content_type]["title"],
                    folder=folder_name,
                    url=f"{self.client.base_url}/{content_type}s/{content_id}",
                    tile_type=(
                        self._get_tile_type(result)
                        if content_type == "dashboard"
                        else None
                    ),
                    tile_title=(
                        result[self._get_tile_type(result)]["title"]
                        if content_type == "dashboard"
                        else None
                    ),
                )
                if explore and content_error not in explore.errors:
                    explore.errors.append(content_error)
                    content_errors.append(content_error)
                elif model and content_error not in model.errors:
                    model.errors.append(content_error)
                    content_errors.append(content_error)

        return content_errors
