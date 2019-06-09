from typing import Sequence, List, Dict, Any, Optional
import re
from fonz.utils import compose_url
from fonz.logger import GLOBAL_LOGGER as logger
from fonz.exceptions import SqlError
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
        self.messages: List[str] = []

        logger.debug("Instantiated Fonz object for url: {}".format(self.base_url))

    def connect(self) -> None:
        """Authenticate, start a dev session, check out specified branch."""

        logger.info("Authenticating Looker credentials. \n")

        response = self.session.post(
            url=compose_url(self.base_url, path=["login"]),
            json={"client_id": self.client_id, "client_secret": self.client_secret},
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

    def get_dimensions(self, model: str, explore_name: str) -> List[str]:
        """Get dimensions for an explore from the LookmlModel endpoint."""

        logger.debug(f"Getting dimensions for {explore_name}")

        response = self.session.get(
            url=compose_url(
                self.base_url, path=["lookml_models", model, "explores", explore_name]
            )
        )
        response.raise_for_status()

        dimensions = []

        for dimension in response.json()["fields"]["dimensions"]:
            if "fonz: ignore" not in dimension["sql"]:
                dimensions.append(dimension["name"])

        return dimensions

    def create_query(self, model: str, explore_name: str, dimensions: List[str]) -> int:
        """Build a Looker query using all the specified dimensions."""

        logger.debug(f"Creating query for {explore_name}")

        response = self.session.post(
            url=compose_url(self.base_url, path=["queries"]),
            json={
                "model": model,
                "view": explore_name,
                "fields": dimensions,
                "limit": 1,
            },
        )
        response.raise_for_status()
        query_id = response.json()["id"]

        return query_id

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
        """Collect the SQL string for a Looker query."""
        logger.debug("Getting SQL for query {}".format(query_id))

        query = self.session.get(
            url=compose_url(self.base_url, path=["queries", query_id, "run", "sql"])
        )

        return query.text

    def validate_explore(
        self, model: str, explore_name: str, dimensions: List[str]
    ) -> None:
        """Query selected dimensions in an explore and return any errors."""
        query_id = self.create_query(model, explore_name, dimensions)
        result = self.run_query(query_id)
        logger.debug(result)
        if not result:
            return
        elif "looker_error" in result[0]:
            error_message = result[0]["looker_error"]
            raise SqlError(query_id, explore_name, error_message)
        else:
            return

    def handle_sql_error(
        self, query_id: int, message: str, explore_name: str, show_sql: bool = True
    ) -> None:
        """Log and save SQL snippet and error message for later."""
        line_number = parse_error_line_number(message)
        sql = self.get_query_sql(query_id)
        sql = sql.replace("\n\n", "\n")
        filename = "./logs/{}.sql".format(explore_name)
        with open(filename, "w+") as file:
            file.write(sql)
        full_message = f"Error in explore {explore_name}: {message}"
        if show_sql:
            sql_context = extract_sql_context(sql, line_number)
            full_message = full_message + "\n\n" + sql_context
        self.messages.append(full_message)
        logger.debug(full_message)

    def validate_content(self) -> JsonDict:
        """Validate all content and return any JSON errors."""
        pass


def mark_line(lines: Sequence, line_number: int, char: str = "*") -> List:
    """For a list of strings, mark a specified line with a prepended character."""
    marked = []
    for i, line in enumerate(lines):
        if i == line_number:
            marked.append(char + " " + line)
        else:
            marked.append("| " + line)
    return marked


def extract_sql_context(sql: str, line_number: int, window_size: int = 2) -> str:
    """Extract a line of SQL with a specified amount of surrounding context."""
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
    """Extract the line number for a SQL error from the error message."""
    BQ_LINE_NUM_PATTERN = r"at \[(\d+):\d+\]"
    try:
        line_number = re.findall(BQ_LINE_NUM_PATTERN, error_message)[0]
    except IndexError:
        pass  # Insert patterns for other data warehouses
    else:
        line_number = int(line_number)

    return line_number
