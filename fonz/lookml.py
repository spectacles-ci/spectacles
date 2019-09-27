import re
from typing import List, Sequence, Optional
from fonz.exceptions import SqlError


class LookMlObject:
    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name})"


class Dimension(LookMlObject):
    def __init__(self, name: str, type: str, sql: str, url: str):
        self.name = name
        self.type = type
        self.sql = sql
        self.url = url
        self.error: Optional[SqlError] = None
        if re.search(r"fonz\s*:\s*ignore", sql, re.IGNORECASE):
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
            and self.type == other.type
            and self.url == other.url
        )

    @property
    def errored(self):
        return bool(self.error)

    @errored.setter
    def errored(self, value):
        raise AttributeError(
            "Cannot assign to 'errored' property of a Dimension instance. "
            "For a dimension to be considered errored, it must have a SqlError "
            "in its 'error' property."
        )

    @classmethod
    def from_json(cls, json_dict):
        name = json_dict["name"]
        type = json_dict["type"]
        sql = json_dict["sql"]
        url = json_dict["lookml_link"]
        return cls(name, type, sql, url)


class Explore(LookMlObject):
    def __init__(self, name: str, dimensions: List[Dimension] = None):
        self.name = name
        self.dimensions = [] if dimensions is None else dimensions
        self.error: Optional[SqlError] = None

    def __eq__(self, other):
        if not isinstance(other, Explore):
            return NotImplemented

        return self.name == other.name and self.dimensions == other.dimensions

    @property
    def errored(self):
        return bool(self.error) or any(
            dimensions.errored for dimensions in self.dimensions
        )

    @errored.setter
    def errored(self, value: bool):
        if not isinstance(value, bool):
            raise TypeError("Value for errored must be boolean.")
        for dimensions in self.dimensions:
            dimensions.errored = value

    def get_errored_dimensions(self):
        for dimension in self.dimensions:
            if dimension.errored:
                yield dimension

    @classmethod
    def from_json(cls, json_dict):
        name = json_dict["name"]
        return cls(name)

    def add_dimension(self, dimension: Dimension):
        self.dimensions.append(dimension)


class Model(LookMlObject):
    def __init__(self, name: str, project: str, explores: List[Explore]):
        self.name = name
        self.project = project
        self.explores = explores

    def __eq__(self, other):
        if not isinstance(other, Model):
            return NotImplemented

        return (
            self.name == other.name
            and self.project == other.project
            and self.explores == other.explores
        )

    @property
    def errored(self):
        return any(explore.errored for explore in self.explores)

    @errored.setter
    def errored(self, value: bool):
        if not isinstance(value, bool):
            raise TypeError("Value for errored must be boolean.")
        for explore in self.explores:
            explore.errored = value

    def get_errored_explores(self):
        for explore in self.explores:
            if explore.errored:
                yield explore

    @classmethod
    def from_json(cls, json_dict):
        name = json_dict["name"]
        project = json_dict["project_name"]
        explores = [Explore.from_json(d) for d in json_dict["explores"]]
        return cls(name, project, explores)


class Project(LookMlObject):
    def __init__(self, name, models: Sequence[Model]):
        self.name = name
        self.models = models

    def __eq__(self, other):
        if not isinstance(other, Project):
            return NotImplemented

        return self.name == other.name and self.models == other.models

    @property
    def errored(self):
        return any(model.errored for model in self.models)

    @errored.setter
    def errored(self, value: bool):
        if not isinstance(value, bool):
            raise TypeError("Value for errored must be boolean.")
        for model in self.models:
            model.errored = value

    def get_errored_models(self):
        for model in self.models:
            if model.errored:
                yield model

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name}, models={self.models})"
