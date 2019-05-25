from typing import Sequence, List, Dict, Any
from utils import compose_url
import requests
import logging

JsonDict = Dict[str, Any]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')


class Fonz:

    def __init__(self, url: str, client_id: str, client_secret: str,
                 project: str, branch: str, port: int, api: str):
        """Instantiate Fonz and save authentication details and branch."""
        if url[-1] == '/':
            self.url = '{}:{}/api/{}/'.format(url[:-1], port, api)
        else:
            self.url = '{}:{}/api/{}/'.format(url, port, api)

        self.client_id = client_id
        self.client_secret = client_secret
        self.branch = branch
        self.project = project
        self.client = None
        self.headers = None

        logging.info('Instantiated Fonz object for url: {}'.format(url))

    def connect(self) -> None:
        """Authenticate, start a dev session, check out specified branch."""

        login = requests.post(
            url=compose_url(self.url, 'login'),
            data={
                'client_id': self.client_id,
                'client_secret': self.client_secret
                })

        access_token = login.json()['access_token']
        self.headers = {'Authorization': 'token {}'.format(access_token)}

        update_session = requests.patch(
            url=compose_url(self.url, 'session'),
            headers=self.headers,
            json={'workspace_id': 'dev'})

        update_branch = requests.put(
            url=compose_url(self.url, 'projects', self.project, 'git_branch'),
            headers=self.headers,
            json={'name': self.branch})

    def get_dimensions(explore: str) -> List[str]:
        """Get dimensions for an explore from the LookmlModel endpoint."""
        pass

    def create_query(dimensions: Sequence) -> str:
        """Build a Looker query using all the specified dimensions."""
        pass

    def run_query(query_id) -> JsonDict:
        """Run a Looker query by ID and return the JSON result."""
        pass

    def validate_content() -> JsonDict:
        """Validate all content and return any JSON errors."""
        pass
