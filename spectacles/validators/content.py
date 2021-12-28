from typing import List, Optional, Any, Dict
from spectacles.client import LookerClient
from spectacles.exceptions import ContentError, SpectaclesException
from spectacles.lookml import Explore, Project
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.types import JsonDict


class ContentValidator:
    def __init__(
        self,
        client: LookerClient,
        exclude_personal: bool = False,
        folders: List[str] = None,
    ):
        include_folders = []
        exclude_folders = []
        if folders:
            for folder_id in folders:
                if folder_id.startswith("-"):
                    exclude_folders.append(int(folder_id[1:]))
                else:
                    include_folders.append(int(folder_id))

        logger.debug(f"Including content in folders: {include_folders}")

        self.client = client
        personal_folders = self._get_personal_folders() if exclude_personal else []

        self.excluded_folders: List[int] = personal_folders + (
            self._get_all_subfolders(exclude_folders) if exclude_folders else []
        )
        self.included_folders: List[int] = (
            self._get_all_subfolders(include_folders) if include_folders else []
        )

    def validate(self, project: Project) -> List[ContentError]:
        def is_folder_selected(folder_id: Optional[str]) -> bool:
            if folder_id in self.excluded_folders:
                return False
            if self.included_folders and folder_id not in self.included_folders:
                return False
            else:
                return True

        result = self.client.content_validation()
        project.queried = True

        content_errors: List[ContentError] = []
        for content in result["content_with_errors"]:
            # Skip content dicts if they lack a `look` or `dashboard` key
            try:
                content_type = self._get_content_type(content)
            except KeyError:
                logger.warn(
                    "Warning: Skipping some content because it does not seem to be a "
                    "Dashboard or a Look."
                )
                logger.debug(f"The unidentified content received was: {content}")
                continue

            # Sometimes the content no longer exists, in which case the folder is None
            folder_id: Optional[str] = content[content_type]["folder"].get("id")
            # If exclude_personal isn't specified, personal_folders list is empty
            if not is_folder_selected(folder_id):
                continue
            else:
                errors = self._get_errors_from_result(project, content, content_type)
                content_errors.extend(errors)

        return content_errors

    def _get_personal_folders(self) -> List[int]:
        personal_folders = []
        result = self.client.all_folders()
        for folder in result:
            if folder["is_personal"] or folder["is_personal_descendant"]:
                personal_folders.append(folder["id"])
        return personal_folders

    def _get_all_subfolders(self, input_folders: List[int]) -> List[int]:
        result = []
        all_folders = self.client.all_folders()
        for folder_id in input_folders:
            if not any(folder["id"] == folder_id for folder in all_folders):
                raise SpectaclesException(
                    name="folder-id-input-does-not-exist",
                    title="One of the folders input doesn't exist.",
                    detail=f"Folder {folder_id} is not a valid folder number.",
                )
            result.extend(self._get_subfolders(folder_id, all_folders))
        return result

    def _get_subfolders(self, folder_id: int, all_folders: List[JsonDict]) -> List[int]:
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
        if content["dashboard"]:
            return "dashboard"
        elif content["look"]:
            return "look"
        else:
            raise KeyError("Content type not found. Valid keys are 'look', 'dashboard'")

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
        self, project: Project, result: Dict, content_type: str
    ) -> List[ContentError]:
        content_errors: List[ContentError] = []
        for error in result["errors"]:
            model_name = error["model_name"]
            explore_name = error["explore_name"]
            explore: Optional[Explore] = project.get_explore(
                model=model_name, name=explore_name
            )
            # Skip errors that are not associated with selected explores
            if explore:
                content_id = result[content_type]["id"]
                content_error = ContentError(
                    model=model_name,
                    explore=explore_name,
                    message=error["message"],
                    field_name=error["field_name"],
                    content_type=content_type,
                    title=result[content_type]["title"],
                    space=result[content_type]["space"]["name"],
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
                if content_error not in explore.errors:
                    explore.errors.append(content_error)
                    content_errors.append(content_error)

        return content_errors
