from typing import List, Optional, Any, Dict
from spectacles.validators.validator import Validator
from spectacles.client import LookerClient
from spectacles.exceptions import ContentError
from spectacles.lookml import Explore
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.types import JsonDict


class ContentValidator(Validator):
    def __init__(
        self,
        client: LookerClient,
        project: str,
        exclude_personal: bool = False,
        exclude_folders: List[int] = [],
        include_folders: List[int] = [],
    ):
        super().__init__(client, project)
        personal_folders = self._get_personal_folders() if exclude_personal else []

        self.excluded_folders: List[int] = personal_folders + self._get_all_subfolders(
            exclude_folders
        )
        self.included_folders: List[int] = self._get_all_subfolders(include_folders)

    def _get_personal_folders(self) -> List[int]:
        personal_folders = []
        result = self.client.all_folders(self.project.name)
        for folder in result:
            if folder["is_personal"] or folder["is_personal_descendant"]:
                personal_folders.append(folder["id"])
        return personal_folders

    def _get_all_subfolders(self, input_folders: List[int]) -> List[int]:
        result = []
        all_folders = self.client.all_folders(self.project.name)
        for folder in input_folders:
            result.extend(self._get_subfolders(folder, all_folders))
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

    def is_selected(self, folder_id: Optional[str]) -> bool:
        if folder_id in self.excluded_folders:
            return False
        if self.included_folders and folder_id not in self.included_folders:
            return False
        else:
            return True

    def validate(self) -> Dict[str, Any]:
        result = self.client.content_validation()
        self.project.queried = True

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
            if not self.is_selected(folder_id):
                continue
            else:
                self._handle_content_result(content, content_type)

        return self.project.get_results(validator="content")

    @staticmethod
    def _get_content_type(content: Dict[str, Any]) -> str:
        if content["dashboard"]:
            return "dashboard"
        elif content["look"]:
            return "look"
        else:
            raise KeyError("Content type not found. Valid keys are 'look', 'dashboard'")

    def _handle_content_result(self, content: Dict, content_type: str) -> None:
        for error in content["errors"]:
            model_name = error["model_name"]
            explore_name = error["explore_name"]
            explore: Optional[Explore] = self.project.get_explore(
                model=model_name, name=explore_name
            )
            # Skip errors that are not associated with selected explores
            if explore is None:
                continue

            content_id = content[content_type]["id"]
            content_error = ContentError(
                model=model_name,
                explore=explore_name,
                message=error["message"],
                field_name=error["field_name"],
                content_type=content_type,
                title=content[content_type]["title"],
                space=content[content_type]["space"]["name"],
                url=f"{self.client.base_url}/{content_type}s/{content_id}",
            )
            if content_error not in explore.errors:
                explore.errors.append(content_error)
