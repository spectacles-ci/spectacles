from typing import Sequence, List, Dict, Any, Optional
from fonz.utils import compose_url
import requests
import logging
import sys

JsonDict = Dict[str, Any]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


class Fonz:
    def __init__(
        self,
        url: str,
        client_id: str,
        client_secret: str,
        port: int,
        api: str,
        project: str = None,
        branch: str = None,
    ):
        """Instantiate Fonz and save authentication details and branch."""
        self.base_url = "{}:{}/api/{}/".format(url.rstrip("/"), port, api)
        self.client_id = client_id
        self.client_secret = client_secret
        self.branch = branch
        self.project = project
        self.client = None
        self.session = requests.Session()

        logging.info("Instantiated Fonz object for url: {}".format(url))

    def connect(self) -> None:
        """Authenticate, start a dev session, check out specified branch."""

        logging.info("Authenticating Looker credentials.")

        response = self.session.post(
            url=compose_url(self.base_url, path=["login"]),
            data={"client_id": self.client_id, "client_secret": self.client_secret},
        )
        response.raise_for_status()

        access_token = response.json()["access_token"]
        self.session.headers = {"Authorization": "token {}".format(access_token)}

    def update_session(self) -> None:

        logging.info("Updating session to use development workspace.")

        response = self.session.patch(
            url=compose_url(self.base_url, path=["session"]),
            json={"workspace_id": "dev"},
        )
        response.raise_for_status()

        logging.info("Setting git branch to: {}".format(self.branch))

        response = self.session.put(
            url=compose_url(
                self.base_url, path=["projects", self.project, "git_branch"]
            ),
            json={"name": self.branch},
        )
        response.raise_for_status()

    def get_explores(self) -> List[JsonDict]:
        """Get all explores from the LookmlModel endpoint."""

        logging.info("Getting all explores in Looker instance.")

        response = self.session.get(
            url=compose_url(self.base_url, path=["lookml_models"])
        )
        response.raise_for_status()

        explores = []

        logging.info("Filtering explores for project: {}".format(self.project))

        for model in response.json():
            if model["project_name"] == self.project:
                for explore in model["explores"]:
                    explores.append(
                        {"model": model["name"], "explore": explore["name"]}
                    )

        return explores

    def get_dimensions(self, model: str, explore: str) -> List[str]:
        """Get dimensions for an explore from the LookmlModel endpoint."""

        logging.info("Getting dimensions for {}".format(explore))

        response = self.session.get(
            url=compose_url(
                self.base_url, path=["lookml_models", model, "explores", explore]
            )
        )
        response.raise_for_status()

        dimensions = []

        for dimension in response.json()["fields"]["dimensions"]:
            dimensions.append(dimension["name"])

        return dimensions

    def create_query(self, model: str, explore: str, dimensions: List[str]) -> int:
        """Build a Looker query using all the specified dimensions."""

        logging.info("Creating query for {}".format(explore))

        response = self.session.post(
            url=compose_url(self.base_url, path=["queries"]),
            json={"model": model, "view": explore, "fields": dimensions, "limit": 1},
        )
        response.raise_for_status()

        query_id = response.json()["id"]

        return query_id

    def run_query(self, query_id: int) -> List[JsonDict]:
        """Run a Looker query by ID and return the JSON result."""

        logging.info("Running query {}".format(query_id))

        response = self.session.get(
            url=compose_url(self.base_url, path=["queries", query_id, "run", "json"])
        )
        response.raise_for_status()
        query_result = response.json()

        return query_result

    def validate_explore(self, query_id: int) -> JsonDict:
        """Take explores and runs a query with all dimensions."""

        result = {}

        query_result = self.run_query(query_id)

        if len(query_result) == 0:
            result["failed"] = False

        elif "looker_error" in query_result[0]:
            result["failed"] = True
            result["error"] = query_result[0]["looker_error"]

        else:
            result["failed"] = False

        return result

    def print_results(self, explores: List[JsonDict]) -> bool:
        """Prints errors and returns whether errors were present"""
        pass

    def validate_content(self) -> JsonDict:
        """Validate all content and return any JSON errors."""
        pass
