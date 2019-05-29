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
        self.url = "{}:{}/api/{}/".format(url.rstrip("/"), port, api)
        self.client_id = client_id
        self.client_secret = client_secret
        self.branch = branch
        self.project = project
        self.client = None
        self.headers = None  # type: Optional[JsonDict]

        logging.info("Instantiated Fonz object for url: {}".format(url))

    def connect(self) -> None:
        """Authenticate, start a dev session, check out specified branch."""

        logging.info("Authenticating Looker credentials.")

        login = requests.post(
            url=compose_url(self.url, path=["login"]),
            data={"client_id": self.client_id, "client_secret": self.client_secret},
        )

        access_token = login.json()["access_token"]
        self.headers = {"Authorization": "token {}".format(access_token)}

    def update_session(self) -> None:

        logging.info("Updating session to use development workspace.")

        update_session = requests.patch(
            url=compose_url(self.url, path=["session"]),
            headers=self.headers,
            json={"workspace_id": "dev"},
        )

        logging.info("Setting git branch to: {}".format(self.branch))

        update_branch = requests.put(
            url=compose_url(self.url, path=["projects", self.project, "git_branch"]),
            headers=self.headers,
            json={"name": self.branch},
        )

    def get_explores(self) -> List[JsonDict]:
        """Get all explores from the LookmlModel endpoint."""

        logging.info("Getting all explores in Looker instance.")

        models = requests.get(
            url=compose_url(self.url, path=["lookml_models"]), headers=self.headers
        )

        explores = []

        logging.info("Filtering explores for project: {}".format(self.project))

        for model in models.json():
            if model["project_name"] == self.project:
                for explore in model["explores"]:
                    explores.append(
                        {"model": model["name"], "explore": explore["name"]}
                    )

        return explores

    def get_explore_dimensions(self, explore: JsonDict) -> List[str]:
        """Get dimensions for an explore from the LookmlModel endpoint."""

        logging.info("Getting dimensions for {}".format(explore["explore"]))

        lookml_explore = requests.get(
            url=compose_url(
                self.url,
                path=[
                    "lookml_models",
                    explore["model"],
                    "explores",
                    explore["explore"],
                ],
            ),
            headers=self.headers,
        )

        dimensions = []

        for dimension in lookml_explore.json()["fields"]["dimensions"]:
            dimensions.append(dimension["name"])

        return dimensions

    def get_dimensions(self, explores: List[JsonDict]) -> List[JsonDict]:
        """Finds the dimensions for all explores"""
        for explore in explores:
            explore["dimensions"] = self.get_explore_dimensions(explore)

        return explores

    def create_query(self, explore: JsonDict) -> int:
        """Build a Looker query using all the specified dimensions."""

        logging.info("Creating query for {}".format(explore["explore"]))

        query = requests.post(
            url=compose_url(self.url, path=["queries"]),
            headers=self.headers,
            json={
                "model": explore["model"],
                "view": explore["explore"],
                "fields": explore["dimensions"],
                "limit": 1,
            },
        )

        query_id = query.json()["id"]

        return query_id

    def run_query(self, query_id: int) -> List[JsonDict]:
        """Run a Looker query by ID and return the JSON result."""

        logging.info("Running query {}".format(query_id))

        query = requests.get(
            url=compose_url(self.url, path=["queries", query_id, "run", "json"]),
            headers=self.headers,
        )

        return query.json()

    def validate_explores(self, explores: List[JsonDict]) -> List[JsonDict]:
        """Take explores and runs a query with all dimensions."""

        for explore in explores:
            query_id = self.create_query(explore)
            query_result = self.run_query(query_id)

            if len(query_result) == 0:
                explore["failed"] = False

            elif "looker_error" in query_result[0]:
                logging.info(
                    "Error in explore {}: {}".format(
                        explore["explore"], query_result[0]["looker_error"]
                    )
                )
                explore["failed"] = True
                explore["error"] = query_result[0]["looker_error"]

            else:
                explore["failed"] = False

        return explores

    def print_results(self, explores: List[JsonDict]) -> bool:
        """Prints errors and returns whether errors were present"""
        pass

    def validate_content(self) -> JsonDict:
        """Validate all content and return any JSON errors."""
        pass
