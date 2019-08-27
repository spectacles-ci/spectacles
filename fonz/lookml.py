import re
from typing import List, Optional
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
        self.errored = False
        self.error: Optional[SqlError] = None
        self.query_id: Optional[int] = None
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
        self.errored = False
        self.error: Optional[SqlError] = None
        self.query_id: Optional[int] = None

    def __eq__(self, other):
        if not isinstance(other, Explore):
            return NotImplemented

        return self.name == other.name and self.dimensions == other.dimensions

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
        self.errored = False

    def __eq__(self, other):
        if not isinstance(other, Model):
            return NotImplemented

        return (
            self.name == other.name
            and self.project == other.project
            and self.explores == other.explores
        )

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
    def __init__(self, name, models: List[Model]):
        self.name = name
        self.models = models

    def __eq__(self, other):
        if not isinstance(other, Project):
            return NotImplemented

        return self.name == other.name and self.models == other.models

    def get_errored_models(self):
        for model in self.models:
            if model.errored:
                yield model

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name}, models={self.models})"
