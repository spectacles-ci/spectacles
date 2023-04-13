from typing import Dict, Any, Optional
from spectacles.client import LookerClient
from spectacles.exceptions import LookMLError
import httpx

# Define constants for severity levels
SUCCESS = 0
INFO = 10
WARNING = 20
ERROR = 30
FATAL = 40

NAME_TO_LEVEL = {
    "success": SUCCESS,
    "info": INFO,
    "warning": WARNING,
    "error": ERROR,
    "fatal": FATAL,
}


class LookMLValidator:
    """Runs LookML validator for a given project.

    Args:
        client: Looker API client.
        project: Name of the LookML project to validate.

    """

    def __init__(self, client: LookerClient):
        self.client = client

    async def validate(self, project: str, severity: str = "warning") -> Dict[str, Any]:
        severity_level: int = NAME_TO_LEVEL[severity]
        validation_results = await self.client.cached_lookml_validation(project)
        if not validation_results or validation_results.get("stale"):
            try:
                validation_results = await self.client.partial_lookml_validation(
                    project
                )
            # If Looker ever removes this undocumented endpoint,
            # fallback to full validation
            except httpx.HTTPStatusError as http_error:
                if http_error.response.status_code == 404:
                    validation_results = await self.client.lookml_validation(project)
                else:
                    raise http_error

        errors = []
        lookml_url: Optional[str] = None
        for error in validation_results["errors"]:
            if error["file_path"]:
                lookml_url = (
                    self.client.base_url
                    + "/projects/"
                    + project
                    + "/files/"
                    + "/".join(error["file_path"].split("/")[1:])
                )
                if error["line_number"]:
                    lookml_url += "?line=" + str(error["line_number"])

            lookml_error = LookMLError(
                model=error["model_id"],
                explore=error["explore"],
                field_name=error["field_name"],
                message=error["message"],
                severity=error["severity"],
                lookml_url=lookml_url,
                line_number=error["line_number"],
                file_path=error["file_path"],
            )
            errors.append(lookml_error)

        if any(NAME_TO_LEVEL[e.metadata["severity"]] >= severity_level for e in errors):
            status = "failed"
        else:
            status = "passed"

        result = {
            "validator": "lookml",
            "errors": [error.to_dict() for error in errors],
            "status": status,
        }
        return result
