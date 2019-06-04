from typing import Sequence, List, Dict, Any, Optional
from fonz.utils import compose_url
from fonz.logger import GLOBAL_LOGGER as logger
from fonz.printer import (
    print_start, print_fail, print_pass, print_error,
    print_stats, print_progress)
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
        self.model = model
        self.branch = branch
        self.project = project
        self.client = None
        self.session = requests.Session()

        logger.debug('Instantiated Fonz object for url: {}'.format(url))

    def connect(self) -> None:
        """Authenticate, start a dev session, check out specified branch."""

        logger.info('Authenticating Looker credentials. \n')

        response = self.session.post(
            url=compose_url(self.base_url, path=["login"]),
            data={"client_id": self.client_id, "client_secret": self.client_secret},
        )
        response.raise_for_status()

        access_token = response.json()["access_token"]
        self.session.headers = {"Authorization": "token {}".format(access_token)}

    def update_session(self) -> None:

        logger.debug('Updating session to use development workspace.')

        response = self.session.patch(
            url=compose_url(self.base_url, path=["session"]),
            json={"workspace_id": "dev"},
        )
        response.raise_for_status()

        logger.debug('Setting git branch to: {}'.format(self.branch))

        response = self.session.put(
            url=compose_url(
                self.base_url, path=["projects", self.project, "git_branch"]
            ),
            json={"name": self.branch},
        )
        response.raise_for_status()

    def get_explores(self) -> List[JsonDict]:
        """Get all explores from the LookmlModel endpoint."""

        logger.debug('Getting all explores in Looker instance.')

        response = self.session.get(
            url=compose_url(self.base_url, path=["lookml_models"])
        )
        response.raise_for_status()

        explores = []

        logger.debug('Filtering explores for project: {}'.format(self.project))
  
        for model in response.json():
            if model["project_name"] == self.project:
                for explore in model["explores"]:
                    explores.append(
                        {"model": model["name"], "explore": explore["name"]}
                    )

        return explores

    def get_explore_dimensions(self, explore: JsonDict) -> List[str]:
        """Get dimensions for an explore from the LookmlModel endpoint."""

        logger.debug('Getting dimensions for {}'.format(explore['explore']))

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
            if 'fonz_ignore' not in dimension['sql']:
                dimensions.append(dimension['name'])

        return dimensions

    def get_dimensions(self, explores: List[JsonDict]) -> List[JsonDict]:
        """Finds the dimensions for all explores"""

        total = len(explores)
        print_progress(0, total, prefix='Finding Dimensions')

        for index, explore in enumerate(explores):
            explore['dimensions'] = self.get_explore_dimensions(explore)
            print_progress(index+1, total, prefix='Finding Dimensions')

        logger.info('Collected dimensions for each explores.')
        return explores

    def create_query(self, explore: JsonDict) -> JsonDict:
        """Build a Looker query using all the specified dimensions."""

        logger.debug('Creating query for {}'.format(explore['explore']))

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

        query_id = response.json()['id']
        query_url = response.json()['share_url']

        return {'id': query_id, 'url': query_url}

    def run_query(self, query_id: int) -> List[JsonDict]:
        """Run a Looker query by ID and return the JSON result."""

        logger.debug('Running query {}'.format(query_id))

        response = self.session.get(
            url=compose_url(self.base_url, path=["queries", query_id, "run", "json"])
        )
        response.raise_for_status()
        query_result = response.json()

        return query_result

    def get_query_sql(self, query_id: int) -> str:

        logger.debug('Getting SQL for query {}'.format(query_id))

        query = requests.get(
            url=compose_url(self.url, 'queries', query_id, 'run', 'sql'),
            headers=self.headers)

        return query.text

    def validate_explores(self, explores: List[JsonDict]) -> List[JsonDict]:
        """Take explores and runs a query with all dimensions."""

        total = len(explores)

        for index, explore in enumerate(explores):

            index += 1
            print_start(explore, index, total)

            query = self.create_query(explore)
            explore['query_url'] = query['url']
            query_result = self.run_query(query['id'])
            query_sql = self.get_query_sql(query['id'])

            logger.debug(query_result)
            logger.debug(query_sql)

            file_name = "./logs/{}.sql".format(explore['explore'])
            with open(file_name, 'w') as stream:
                stream.write(query_sql)

            if len(query_result) == 0:

                explore['failed'] = False
                print_pass(explore, index, total)

            elif 'looker_error' in query_result[0]:
                logger.debug(
                  'Error in explore {}: {}'.format(
                    explore['explore'], query_result[0]['looker_error'])
                )
                explore['failed'] = True
                explore['error'] = query_result[0]['looker_error']
                print_fail(explore, index, total)

            else:
                explore['failed'] = False
                print_pass(explore, index, total)

        return explores

    def handle_errors(self, explores: List[JsonDict]) -> None:
        """Prints errors and returns whether errors were present"""

        total = len(explores)
        errors = 0

        for explore in explores:
            if explore['failed']:
                errors += 1
                print_error(explore)

        print_stats(errors, total)

        if errors > 0:
            sys.exit(1)

    def validate_content(self) -> JsonDict:
        """Validate all content and return any JSON errors."""
        pass
