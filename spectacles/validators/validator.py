from typing import Optional, List
from abc import ABC, abstractmethod
from spectacles.client import LookerClient
from spectacles.lookml import Project, Model, Dimension
from spectacles.select import is_selected
from spectacles.exceptions import LookMlNotFound


class Validator(ABC):  # pragma: no cover
    """Defines abstract base interface for validators.

    Not intended to be used directly, only inherited.

    Attributes:
        client: Looker API client.

    """

    def __init__(self, client: LookerClient, project: str):
        self.client = client
        self.project = Project(project, models=[])

    @abstractmethod
    def validate(self):
        raise NotImplementedError

    def build_project(
        self,
        selectors: Optional[List[str]] = None,
        exclusions: Optional[List[str]] = None,
        build_dimensions: bool = False,
    ) -> None:
        """Creates an object representation of the project's LookML.

        Args:
            selectors: List of selector strings in 'model_name/explore_name' format.
                The '*' wildcard selects all models or explores. For instance,
                'model_name/*' would select all explores in the 'model_name' model.

        """
        # Assign default values for selectors and exclusions
        if selectors is None:
            selectors = ["*/*"]
        if exclusions is None:
            exclusions = []

        all_models = [
            Model.from_json(model)
            for model in self.client.get_lookml_models(
                fields=["name", "project_name", "explores"]
            )
        ]
        project_models = [
            model for model in all_models if model.project_name == self.project.name
        ]

        if not project_models:
            raise LookMlNotFound(
                name="project-models-not-found",
                title="No configured models found for the specified project.",
                detail=(
                    f"Go to {self.client.base_url}/projects and confirm "
                    "a) at least one model exists for the project and "
                    "b) it has an active configuration."
                ),
            )

        for model in project_models:
            model.explores = [
                explore
                for explore in model.explores
                if is_selected(model.name, explore.name, selectors, exclusions)
            ]

            if build_dimensions:
                for explore in model.explores:
                    dimensions_json = self.client.get_lookml_dimensions(
                        model.name, explore.name
                    )
                    for dimension_json in dimensions_json:
                        dimension = Dimension.from_json(
                            dimension_json, model.name, explore.name
                        )
                        dimension.url = self.client.base_url + dimension.url
                        if not dimension.ignore:
                            explore.add_dimension(dimension)

        self.project.models = [
            model for model in project_models if len(model.explores) > 0
        ]
