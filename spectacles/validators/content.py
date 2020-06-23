from typing import List, Optional, Any, Dict
from spectacles.validators.validator import Validator
from spectacles.client import LookerClient
from spectacles.exceptions import ContentError
from spectacles.lookml import Explore
from spectacles.logger import GLOBAL_LOGGER as logger


class ContentValidator(Validator):
    def __init__(
        self, client: LookerClient, project: str, exclude_personal: bool = False
    ):
        super().__init__(client, project)
        personal_folders = self._get_personal_folders() if exclude_personal else []
        self.personal_folders: List[int] = personal_folders

    def _get_personal_folders(self) -> List[int]:
        personal_folders = []
        result = self.client.all_folders(self.project.name)
        for folder in result:
            if folder["is_personal"] or folder["is_personal_descendant"]:
                personal_folders.append(folder["id"])
        return personal_folders

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

            # If exclude_personal isn't specified, personal_folders list is empty
            if content[content_type]["folder"]["id"] in self.personal_folders:
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
