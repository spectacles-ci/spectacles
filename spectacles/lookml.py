import re
from typing import List, Sequence, Optional
from spectacles.exceptions import SqlError


class LookMlObject:
    def __init__(self):
        self._parent_path = ""
        self._filtered = False

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name})"

    @property
    def path(self):
        return f"{self.parent_path}/{self.name}"

    @property
    def parent_path(self):
        return self._parent_path

    @parent_path.setter
    def parent_path(self, value):
        if not isinstance(value, str):
            raise TypeError("Value for parent_path must be string.")
        self._parent_path = value

    @property
    def filtered(self):
        return bool(self._filtered)

    @filtered.setter
    def filtered(self, value):
        if not isinstance(value, bool):
            raise TypeError("Value for filtered must be boolean.")
        self._filtered = value


class Dimension(LookMlObject):
    def __init__(self, name: str, type: str, sql: str, url: Optional[str]):
        super().__init__()
        self.name = name
        self.type = type
        self.sql = sql
        self.url = url
        self.queried: bool = False
        self.error: Optional[SqlError] = None
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
            and self.type == other.type
            and self.url == other.url
        )

    @property
    def errored(self):
        return bool(self.error) if self.queried else None

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
        super().__init__()
        self.name = name
        self.dimensions = [] if dimensions is None else dimensions
        self.queried: bool = False
        self.error: Optional[SqlError] = None
        for dimension in self.dimensions:
            dimension.parent_path = self.path

    def __eq__(self, other):
        if not isinstance(other, Explore):
            return NotImplemented

        return self.name == other.name and self.dimensions == other.dimensions

    @property
    def errored(self):
        if self.queried:
            return bool(self.error) or any(
                dimension.errored for dimension in self.dimensions
            )
        else:
            return None

    @errored.setter
    def errored(self, value: bool):
        if not isinstance(value, bool):
            raise TypeError("Value for errored must be boolean.")
        for dimensions in self.dimensions:
            dimensions.errored = value

    @property
    def queried(self):
        return any(dimension.queried for dimension in self.dimensions)

    @queried.setter
    def queried(self, value: bool):
        if not isinstance(value, bool):
            raise TypeError("Value for queried must be boolean.")
        for dimensions in self.dimensions:
            dimensions.queried = value

    def get_errored_dimensions(self):
        for dimension in self.dimensions:
            if dimension.errored:
                yield dimension

    @property
    def filtered(self) -> bool:
        return bool(self._filtered)

    @filtered.setter
    def filtered(self, value):
        if not isinstance(value, bool):
            raise TypeError("Value for filtered must be boolean.")
        self._filtered = value
        for dimension in self.dimensions:
            dimension.filtered = value

    @property
    def has_unfiltered_dimensions(self) -> bool:
        return any(d for d in self.dimensions if not d.filtered)

    @classmethod
    def from_json(cls, json_dict):
        name = json_dict["name"]
        return cls(name)

    def add_dimension(self, dimension: Dimension):
        dimension.parent_path = self.path
        self.dimensions.append(dimension)

    def pretty(self, indent=0):
        ret = "\t" * indent
        ret += f"Explore(name={self.name}, "
        num_dims = len(self.dimensions)
        ret += f"{num_dims} dimension"
        if num_dims == 0:
            ret += "s)\n"
        else:
            ret += f"{'s' if num_dims > 1 else ''}:\n"
            for dimension in sorted(self.dimensions, key=lambda x: x.name):
                ret += "\t" * (indent + 1) + str(dimension) + "\n"
            ret += "\t" * indent + ")\n"
        return ret


class Model(LookMlObject):
    def __init__(self, name: str, project: str, explores: List[Explore]):
        super().__init__()
        self.name = name
        self.project = project
        for explore in explores:
            explore.parent_path = self.path
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
        if self.queried:
            return any(explore.errored for explore in self.explores)
        else:
            return None

    @errored.setter
    def errored(self, value: bool):
        if not isinstance(value, bool):
            raise TypeError("Value for errored must be boolean.")
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

    def get_errored_explores(self):
        for explore in self.explores:
            if explore.errored:
                yield explore

    @property
    def filtered(self) -> bool:
        return bool(self._filtered)

    @filtered.setter
    def filtered(self, value):
        if not isinstance(value, bool):
            raise TypeError("Value for filtered must be boolean.")
        self._filtered = value
        for explore in self.explores:
            explore.filtered = value

    @property
    def has_unfiltered_explores(self) -> bool:
        return any(e for e in self.explores if not e.filtered)

    @property
    def has_unfiltered_dimensions(self) -> bool:
        return any(e for e in self.explores if not e.has_unfiltered_dimensions)

    @property
    def has_unfiltered_children(self) -> bool:
        return self.has_unfiltered_explores or self.has_unfiltered_dimensions

    def pretty(self, indent=0):
        ret = "\t" * indent
        ret += f"Model(name={self.name}, "
        num_explores = len(self.explores)
        ret += f"{num_explores} explore"
        if num_explores == 0:
            ret += "s)\n"
        else:
            ret += f"{'s' if num_explores > 1 else ''}:\n"
            for explore in self.explores:
                ret += explore.pretty(indent + 1) + "\n"
            ret += "\t" * indent + ")\n"
        return ret

    @classmethod
    def from_json(cls, json_dict):
        name = json_dict["name"]
        project = json_dict["project_name"]
        explores = []
        for e_json in json_dict["explores"]:
            explore = Explore.from_json(e_json)
            explore.parent_path = f"{project}/{name}"
            explores.append(explore)
        model = cls(name, project, explores)
        model.parent_path = f"{project}"
        return model


class Project(LookMlObject):
    def __init__(self, name, models: Sequence[Model]):
        super().__init__()
        self.name = name
        self.models = models

    def __eq__(self, other):
        if not isinstance(other, Project):
            return NotImplemented

        return self.name == other.name and self.models == other.models

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

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name}, models={self.models})"

    def paths(self):
        paths = []
        for model in self.models:
            if not model.filtered:
                paths.append(model.path)
            for explore in model.explores:
                if not explore.filtered:
                    paths.append(explore.path)
                for dimension in explore.dimensions:
                    if not dimension.filtered:
                        paths.append(dimension.path)
        return sorted(paths)

    def pretty(self, indent=0):
        ret = "\t" * indent
        ret += f"Project(name={self.name}, "
        num_models = len(self.models)
        ret += f"{num_models} model"
        if num_models == 0:
            ret += "s)\n"
        else:
            ret += f"{'s' if num_models > 1 else ''}:\n"
            for model in sorted(self.models, key=lambda x: x.name):
                ret += model.pretty(indent + 1) + "\n"
            ret += "\t" * indent + ")\n"
        return ret
