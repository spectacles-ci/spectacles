import re
from typing import Dict, List, Sequence, Optional, Any
from spectacles.exceptions import ValidationError
from spectacles.types import QueryMode


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
        sql: str,
        url: Optional[str] = None,
    ):
        self.name = name
        self.model_name = model_name
        self.explore_name = explore_name
        self.type = type
        self.sql = sql
        self.url = url
        self.queried: bool = False
        self.errors: List[ValidationError] = []
        if re.search(r"spectacles\s*:\s*ignore", sql, re.IGNORECASE):
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
        sql = json_dict["sql"]
        url = json_dict["lookml_link"]
        return cls(name, model_name, explore_name, type, sql, url)


class Explore(LookMlObject):
    def __init__(self, name: str, model_name: str, dimensions: List[Dimension] = None):
        self.name = name
        self.model_name = model_name
        self.dimensions = [] if dimensions is None else dimensions
        self.errors: List[ValidationError] = []
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
        return sum(len(model.explores) for model in self.models)

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
        self, validator: str, mode: Optional[QueryMode] = None
    ) -> Dict[str, Any]:
        errors: List[Dict[str, Any]] = []
        tested = []

        def parse_explore_errors(explore):
            if validator != "sql" or mode == "batch":
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
                tested.append(test)

        passed = min((test["passed"] for test in tested), default=True)
        return {
            "validator": validator,
            "status": "passed" if passed else "failed",
            "tested": tested,
            "errors": errors,
        }

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name}, models={self.models})"

    @property
    def number_of_errors(self):
        return sum([model.number_of_errors for model in self.models if model.errored])
