from typing import Sequence, List, Dict, Any, Optional
import re
from fonz.utils import compose_url
from fonz.logger import GLOBAL_LOGGER as logger
from fonz.exceptions import SqlError
from fonz.printer import (
    print_start,
    print_fail,
    print_pass,
    print_error,
    print_stats,
    print_progress,
)
import requests
import sys

JsonDict = Dict[str, Any]


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
        self.messages = []

        logger.debug("Instantiated Fonz object for url: {}".format(self.base_url))

    def connect(self) -> None:
        """Authenticate, start a dev session, check out specified branch."""

        logger.info("Authenticating Looker credentials. \n")

        response = self.session.post(
            url=compose_url(self.base_url, path=["login"]),
            data={"client_id": self.client_id, "client_secret": self.client_secret},
        )
        response.raise_for_status()

        access_token = response.json()["access_token"]
        self.session.headers = {"Authorization": "token {}".format(access_token)}

    def update_session(self) -> None:

        logger.debug("Updating session to use development workspace.")

        response = self.session.patch(
            url=compose_url(self.base_url, path=["session"]),
            json={"workspace_id": "dev"},
        )
        response.raise_for_status()

        logger.debug("Setting git branch to: {}".format(self.branch))

        response = self.session.put(
            url=compose_url(
                self.base_url, path=["projects", self.project, "git_branch"]
            ),
            json={"name": self.branch},
        )
        response.raise_for_status()

    def get_explores(self) -> List[JsonDict]:
        """Get all explores from the LookmlModel endpoint."""

        logger.debug("Getting all explores in Looker instance.")

        response = self.session.get(
            url=compose_url(self.base_url, path=["lookml_models"])
        )
        response.raise_for_status()

        explores = []

        logger.debug("Filtering explores for project: {}".format(self.project))

        for model in response.json():
            if model["project_name"] == self.project:
                for explore in model["explores"]:
                    explores.append(
                        {"model": model["name"], "explore": explore["name"]}
                    )

        return explores

    def get_explore_dimensions(self, explore: JsonDict) -> List[str]:
        """Get dimensions for an explore from the LookmlModel endpoint."""

        logger.debug("Getting dimensions for {}".format(explore["explore"]))

        response = self.session.get(
            url=compose_url(
                self.base_url,
                path=[
                    "lookml_models",
                    explore["model"],
                    "explores",
                    explore["explore"],
                ],
            )
        )
        response.raise_for_status()

        dimensions = []

        for dimension in response.json()["fields"]["dimensions"]:
            if "fonz: ignore" not in dimension["sql"]:
                dimensions.append(dimension["name"])

        return dimensions

    def get_dimensions(self, explores: List[JsonDict]) -> List[JsonDict]:
        """Finds the dimensions for all explores"""

        total = len(explores)
        print_progress(0, total, prefix="Finding Dimensions")

        for index, explore in enumerate(explores):
            explore["dimensions"] = self.get_explore_dimensions(explore)
            print_progress(index + 1, total, prefix="Finding Dimensions")

        logger.info("Collected dimensions for each explores.")
        return explores

    def create_query(self, explore: JsonDict) -> JsonDict:
        """Build a Looker query using all the specified dimensions."""

        logger.debug("Creating query for {}".format(explore["explore"]))

        response = self.session.post(
            url=compose_url(self.base_url, path=["queries"]),
            json={
                "model": explore["model"],
                "view": explore["explore"],
                "fields": explore["dimensions"],
                "limit": 1,
            },
        )
        response.raise_for_status()

        query_id = response.json()["id"]
        query_url = response.json()["share_url"]

        return {"id": query_id, "url": query_url}

    def run_query(self, query_id: int) -> List[JsonDict]:
        """Run a Looker query by ID and return the JSON result."""

        logger.debug("Running query {}".format(query_id))

        response = self.session.get(
            url=compose_url(self.base_url, path=["queries", query_id, "run", "json"])
        )
        response.raise_for_status()
        query_result = response.json()

        return query_result

    def get_query_sql(self, query_id: int) -> str:

        logger.debug("Getting SQL for query {}".format(query_id))

        query = self.session.get(
            url=compose_url(self.base_url, path=["queries", query_id, "run", "sql"])
        )

        return query.text

    def validate_explore(self, explore):
        query = self.create_query(explore)
        result = self.run_query(query["id"])
        logger.debug(result)
        if not result:
            return
        elif "looker_error" in result[0]:
            error_message = result[0]["looker_error"]
            raise SqlError(query["id"], explore["explore"], error_message)
        else:
            return

    def validate_all_explores(self, explores: List[JsonDict]) -> List[JsonDict]:
        """Take explores and runs a query with all dimensions."""

        explore_count = len(explores)
        for index, explore in enumerate(explores):
            index += 1
            print_start(explore, index, explore_count)
            try:
                self.validate_explore(explore)
            except SqlError as error:
                # TODO: Move this into a separate function
                line_number = parse_error_line_number(error.message)
                sql = self.get_query_sql(error.query_id)
                sql = sql.replace("\n\n", "\n")
                filename = "./logs/{}.sql".format(error.explore_name)
                with open(filename, "w+") as file:
                    file.write(sql)
                sql_context = extract_sql_context(sql, line_number)
                full_message = f"Error in explore {error.explore_name}: {error.message}"
                full_message = full_message + "\n\n" + sql_context
                self.messages.append(full_message)
                logger.debug(full_message)
                print_fail(explore, index, explore_count)
            else:
                print_pass(explore, index, explore_count)
        return explores

    def handle_errors(self, explores: List[JsonDict]) -> None:
        """Prints errors and returns whether errors were present"""

        total = len(explores)
        errors = 0

        for message in self.messages:
            errors += 1
            print_error(message)

        print_stats(errors, total)

        if errors > 0:
            sys.exit(1)

    def validate_content(self) -> JsonDict:
        """Validate all content and return any JSON errors."""
        pass


def mark_line(lines: Sequence, line_number: int, char: str = "*") -> List:
    marked = []
    for i, line in enumerate(lines):
        if i == line_number:
            marked.append(char + " " + line)
        else:
            marked.append("| " + line)
    return marked


def extract_sql_context(sql: str, line_number: int, window_size: int = 2) -> str:
    split = sql.split("\n")
    line_number -= 1  # Align with array indexing
    line_start = line_number - (window_size + 1)
    line_end = line_number + window_size
    line_start = line_start if line_start >= 0 else 0
    line_end = line_end if line_end <= len(split) else len(split)

    selected_lines = split[line_start:line_end]
    marked = mark_line(selected_lines, line_number=window_size)
    context = "\n".join(marked)
    return context


def parse_error_line_number(error_message: str) -> int:
    BQ_LINE_NUM_PATTERN = r"at \[(\d+):\d+\]"
    try:
        line_number = re.findall(BQ_LINE_NUM_PATTERN, error_message)[0]
    except IndexError:
        pass  # Insert patterns for other data warehouses
    else:
        line_number = int(line_number)

    return line_number
