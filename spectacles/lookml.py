import re
from typing import Dict, List, Sequence, Optional, Any, Iterable
from spectacles.client import LookerClient
from spectacles.exceptions import ValidationError, LookMlNotFound
from spectacles.types import JsonDict
from spectacles.select import is_selected


class LookMlObject:
    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name})"


class Dimension(LookMlObject):
    def __init__(
        self,
        name: str,
        model_name: str,
        explore_name: str,
        type: str,
        tags: List[str],
        sql: str,
        url: Optional[str] = None,
    ):
        self.name = name
        self.model_name = model_name
        self.explore_name = explore_name
        self.type = type
        self.tags = tags
        self.sql = sql
        self.url = url
        self.queried: bool = False
        self.errors: List[ValidationError] = []

        if (
            re.search(r"spectacles\s*:\s*ignore", sql, re.IGNORECASE)
            or "spectacles: ignore" in tags
        ):
            self.ignore = True
        else:
            self.ignore = False

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(name={self.name}, "
            f"type={self.type}, "
            f"errored={self.errored})"
        )

    def __eq__(self, other):
        if not isinstance(other, Dimension):
            return NotImplemented

        return (
            self.name == other.name
            and self.model_name == other.model_name
            and self.explore_name == other.explore_name
            and self.type == other.type
            and self.url == other.url
        )

    @property
    def errored(self):
        return bool(self.errors) if self.queried else None

    @errored.setter
    def errored(self, value):
        raise AttributeError(
            "Cannot assign to 'errored' property of a Dimension instance. "
            "For a dimension to be considered errored, it must have a ValidationError "
            "in its 'errors' attribute."
        )

    @classmethod
    def from_json(cls, json_dict, model_name, explore_name):
        name = json_dict["name"]
        type = json_dict["type"]
        tags = json_dict["tags"]
        sql = json_dict["sql"]
        url = json_dict["lookml_link"]
        return cls(name, model_name, explore_name, type, tags, sql, url)


class Explore(LookMlObject):
    def __init__(self, name: str, model_name: str, dimensions: List[Dimension] = None):
        self.name = name
        self.model_name = model_name
        self.dimensions = [] if dimensions is None else dimensions
        self.errors: List[ValidationError] = []
        self.successes: List[JsonDict] = []
        self.skipped = False
        self._queried: bool = False

    def __eq__(self, other):
        if not isinstance(other, Explore):
            return NotImplemented

        return (
            self.name == other.name
            and self.model_name == other.model_name
            and self.dimensions == other.dimensions
        )

    @property
    def queried(self):
        if self.dimensions:
            return any(dimension.queried for dimension in self.dimensions)
        else:
            return self._queried

    @queried.setter
    def queried(self, value: bool):
        if not isinstance(value, bool):
            raise TypeError("Value for queried must be boolean.")
        if self.dimensions:
            for dimension in self.dimensions:
                dimension.queried = value
        else:
            self._queried = value

    @property
    def errored(self):
        if self.queried:
            return bool(self.errors) or any(
                dimension.errored for dimension in self.dimensions
            )
        else:
            return None

    @errored.setter
    def errored(self, value):
        raise AttributeError(
            "Cannot assign to 'errored' property of an Explore instance. "
            "For an explore to be considered errored, it must have a ValidationError "
            "in its 'errors' attribute or contain dimensions in an errored state."
        )

    def get_errored_dimensions(self):
        for dimension in self.dimensions:
            if dimension.errored:
                yield dimension

    @classmethod
    def from_json(cls, json_dict, model_name):
        name = json_dict["name"]
        return cls(name, model_name)

    def add_dimension(self, dimension: Dimension):
        self.dimensions.append(dimension)

    @property
    def number_of_errors(self):
        if self.errored:
            if self.errors:
                errors = len(self.errors)
            else:
                errors = sum(
                    len(dimension.errors)
                    for dimension in self.dimensions
                    if dimension.errored
                )
            return errors
        else:
            return 0


class Model(LookMlObject):
    def __init__(self, name: str, project_name: str, explores: List[Explore]):
        self.name = name
        self.project_name = project_name
        self.explores = explores

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name}, models={self.models})"

    def __eq__(self, other):
        if not isinstance(other, Model):
            return NotImplemented

        return (
            self.name == other.name
            and self.project_name == other.project_name
            and self.explores == other.explores
        )

    @property
    def errored(self):
        if self.queried:
            return any(explore.errored for explore in self.explores)
        else:
            return None

    @errored.setter
    def errored(self, value: bool):
        if not isinstance(value, bool):
            raise TypeError("Value for errored must be boolean.")
        if not self.explores:
            raise AttributeError(
                "Cannot assign to 'errored' property because this model does not have any explores."
            )
        for explore in self.explores:
            explore.errored = value

    @property
    def queried(self):
        return any(explore.queried for explore in self.explores)

    @queried.setter
    def queried(self, value: bool):
        if not isinstance(value, bool):
            raise TypeError("Value for queried must be boolean.")
        for explore in self.explores:
            explore.queried = value

    def get_explore(self, name: str) -> Optional[Explore]:
        return next(
            (explore for explore in self.explores if explore.name == name), None
        )

    def get_errored_explores(self):
        for explore in self.explores:
            if explore.errored:
                yield explore

    @classmethod
    def from_json(cls, json_dict):
        name = json_dict["name"]
        project = json_dict["project_name"]
        explores = [
            Explore.from_json(d, model_name=name) for d in json_dict["explores"]
        ]
        return cls(name, project, explores)

    @property
    def number_of_errors(self):
        return sum(
            [explore.number_of_errors for explore in self.explores if explore.errored]
        )


class Project(LookMlObject):
    def __init__(self, name, models: Sequence[Model]):
        self.name = name
        self.models = models

    def __eq__(self, other):
        if not isinstance(other, Project):
            return NotImplemented

        return self.name == other.name and self.models == other.models

    def count_explores(self) -> int:
        """Returns the number of explores in the project, excluding skipped explores."""
        return len([explore for explore in self.iter_explores() if not explore.skipped])

    def iter_explores(self) -> Iterable[Explore]:
        for model in self.models:
            for explore in model.explores:
                yield explore

    @property
    def errored(self):
        if self.queried:
            return any(model.errored for model in self.models)
        else:
            return None

    @errored.setter
    def errored(self, value: bool):
        if not isinstance(value, bool):
            raise TypeError("Value for errored must be boolean.")
        if not self.models:
            raise AttributeError(
                "Cannot assign to 'errored' property because this project does not have any models."
            )
        for model in self.models:
            model.errored = value

    @property
    def queried(self):
        return any(model.queried for model in self.models)

    @queried.setter
    def queried(self, value: bool):
        if not isinstance(value, bool):
            raise TypeError("Value for queried must be boolean.")
        for model in self.models:
            model.queried = value

    def get_errored_models(self):
        for model in self.models:
            if model.errored:
                yield model

    def get_model(self, name: str) -> Optional[Model]:
        return next((model for model in self.models if model.name == name), None)

    def get_explore(self, model: str, name: str) -> Optional[Explore]:
        model_object = self.get_model(model)
        if not model_object:
            return None
        else:
            return model_object.get_explore(name)

    def get_results(
        self, validator: str, fail_fast: Optional[bool] = None
    ) -> Dict[str, Any]:
        errors: List[Dict[str, Any]] = []
        successes: List[Dict[str, Any]] = []
        tested = []

        def parse_explore_errors(explore):
            if validator != "sql" or fail_fast is True:
                errors.extend([error.__dict__ for error in explore.errors])
            else:
                for dimension in explore.dimensions:
                    if dimension.errored:
                        errors.extend([error.__dict__ for error in dimension.errors])

        for model in self.models:
            for explore in model.explores:
                passed = True
                if explore.errored:
                    passed = False
                    parse_explore_errors(explore)
                test: Dict[str, Any] = {
                    "model": model.name,
                    "explore": explore.name,
                    "passed": passed,
                }
                if explore.successes:
                    successes.extend([success for success in explore.successes])

                tested.append(test)

        passed = min((test["passed"] for test in tested), default=True)
        return {
            "validator": validator,
            "status": "passed" if passed else "failed",
            "tested": tested,
            "errors": errors,
            "successes": successes,
        }

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name}, models={self.models})"

    @property
    def number_of_errors(self):
        return sum([model.number_of_errors for model in self.models if model.errored])


def build_dimensions(
    client: LookerClient,
    model_name: str,
    explore_name: str,
) -> List[Dimension]:
    """Creates Dimension objects for all dimensions in a given explore."""
    dimensions_json = client.get_lookml_dimensions(model_name, explore_name)
    dimensions: List[Dimension] = []
    for dimension_json in dimensions_json:
        dimension = Dimension.from_json(dimension_json, model_name, explore_name)
        dimension.url = client.base_url + dimension.url
        if not dimension.ignore:
            dimensions.append(dimension)
    return dimensions


def build_project(
    client: LookerClient,
    name: str,
    filters: Optional[List[str]] = None,
    include_dimensions: bool = False,
) -> Project:
    """Creates an object (tree) representation of a LookML project."""
    if filters is None:
        filters = ["*/*"]

    models = []
    fields = ["name", "project_name", "explores"]
    for lookmlmodel in client.get_lookml_models(fields=fields):
        model = Model.from_json(lookmlmodel)
        if model.project_name == name and model.explores:
            models.append(model)

    if not models:
        raise LookMlNotFound(
            name="project-models-not-found",
            title="No configured models found for the specified project.",
            detail=(
                f"Go to {client.base_url}/projects and confirm "
                "a) at least one model exists for the project and "
                "b) it has an active configuration."
            ),
        )

    for model in models:
        model.explores = [
            explore
            for explore in model.explores
            if is_selected(model.name, explore.name, filters)
        ]

        if include_dimensions:
            for explore in model.explores:
                explore.dimensions = build_dimensions(client, model.name, explore.name)

    project = Project(name, models)
    return project
