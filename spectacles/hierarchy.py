import os
import json
import re
from typing import List
from fnmatch import translate as glob_to_regex
from spectacles.client import LookerClient
from spectacles.lookml import Project, Model, Dimension
from spectacles.logger import GLOBAL_LOGGER as logger


class Hierarchy(object):
    """Hierarchy object representation of a LookML project

    Args:
        client: Looker API client.
        name: Name of the LookML project to retrieve.
        filters: List of globs to filter hierarchy by,
                 multiple globs will be logically OR'd.
    Attributes:
        client: Looker API Client.
        project: Root object of the hierarchy.
        model_count: Number of models in the project.
        explore_count: Number of explores in the project.
        dimension_count: Number of dimensions in the project.
    """

    def __init__(self, client: LookerClient, name: str, filters: List[str] = None):
        self.client = client
        self.name = name
        self.get_all_dimensions = False

        if filters:
            # Ensure the filter path begins with a slash
            filters = list(map(lambda s: re.sub(r"[/]+", "/", f"/{s}"), filters))

            # Because we must explicitly query for the dimensions, if we are
            # given a filter that globs model/exlores but masks dimensions,
            # eg: /*/*/dimension, then we must query the dimensions of all
            # models/explores in order for an exhaustive search.
            self.get_all_dimensions = any(
                re.match(r"^/\*/\*\/.*[^*?].*$", f) for f in filters
            )
            if self.get_all_dimensions:
                logger.debug(
                    "Dimension filtering detected, must retrieve complete hierarchy."
                )

            # Translate shell-like globbing into actual regular expressions
            rx_patterns = [f"{glob_to_regex(f)}" for f in filters]

            # Formulate a compound regex to test the names against
            self.filter_pattern = "|".join("(?:{0})".format(p) for p in rx_patterns)

        # self.build(json_dumpdir="json")
        self.build()

    def _save_json(self, json_obj, filename: str) -> None:
        if json_obj and filename:
            logger.debug(f"Saving {filename}")
            dirname = os.path.dirname(filename)
            if not os.path.exists(dirname):
                logger.debug(f"creating directory {dirname} to store json")
                os.makedirs(dirname)
            with open(filename, "w") as f:
                json.dump(json_obj, f)

    def _filter(self, path: str) -> bool:
        return self.filter_pattern is not None and not re.match(
            self.filter_pattern, path
        )

    def build(self, json_dumpdir: str = None) -> None:
        """Query Looker and build out the hierarchy.
        """
        logger.info(f"Building LookML project hierarchy for project {self.name}")
        logger.info(f"filter pattern: {self.filter_pattern}")
        self.project = Project(self.name, models=[])
        self.model_count = 0
        self.explore_count = 0
        self.dimension_count = 0

        models_json = self.client.get_lookml_models()
        if json_dumpdir:
            self._save_json(models_json, os.path.join(json_dumpdir, "models.json"))

        self.project.models = []
        for model_json in models_json:
            model = Model.from_json(model_json)
            if model.project == self.name:
                model.filtered = self._filter(model.path)
                logger.debug(
                    f"model {model.path} is "
                    + ("filtered" if model.filtered else "not filtered")
                )
                for explore in model.explores:
                    self.explore_count += 1
                    explore.filtered = self._filter(explore.path)
                    logger.debug(
                        f"explore {explore.path} is "
                        + ("filtered" if explore.filtered else "not filtered")
                    )
                    if self.get_all_dimensions or not explore.filtered:
                        dimensions_json = self.client.get_lookml_dimensions(
                            model.name, explore.name
                        )
                        if json_dumpdir:
                            self._save_json(
                                dimensions_json,
                                os.path.join(
                                    json_dumpdir,
                                    f"{model.name}_{explore.name}-dims.json",
                                ),
                            )
                        self.dimension_count += len(dimensions_json)
                        for dimension_json in dimensions_json:
                            dimension = Dimension.from_json(dimension_json)
                            dimension.parent_path = explore.path
                            dimension.url = self.client.base_url + dimension.url
                            dimension.filtered = self._filter(dimension.path)
                            logger.debug(
                                f"dimension {dimension.path} is "
                                + ("filtered" if dimension.filtered else "not filtered")
                            )
                            explore.add_dimension(dimension)

                self.project.models.append(model)

        self.model_count = len(self.project.models)

        logger.info(
            f"Project {self.name} includes "
            f"{self.model_count} model{'' if self.model_count == 1 else 's'}, "
            f"{self.explore_count} explore{'' if self.explore_count == 1 else 's'}, "
            f"{self.dimension_count} "
            f"dimension{'' if self.dimension_count == 1 else 's'}"
        )
