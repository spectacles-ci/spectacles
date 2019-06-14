from typing import List


class LookMlObject:
    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name})"


class Dimension(LookMlObject):
    def __init__(self, name: str, type: str, sql: str, url: str):
        self.name = name
        self.type = type
        self.sql = sql
        self.url = url
        self.ignore = True if "fonz: ignore" in sql else False

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name}, type={self.type})"

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

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name}, models={self.models})"
