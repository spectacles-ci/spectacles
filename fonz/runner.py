from typing import List
from fonz.client import LookerClient
from fonz.validators import SqlValidator


class Runner:
    def __init__(
        self,
        base_url: str,
        project: str,
        branch: str,
        client_id: str,
        client_secret: str,
        port: int = 19999,
        api_version: float = 3.1,
    ):
        self.project = project
        self.client = LookerClient(
            base_url, client_id, client_secret, port, api_version
        )
        self.client.update_session(project, branch)

    def validate_sql(self, selectors: List[str], batch: bool = False) -> List[dict]:
        sql_validator = SqlValidator(self.client, self.project)
        sql_validator.build_project(selectors)
        errors = sql_validator.validate(batch)
        return [error.__dict__ for error in errors]
