from typing import Dict, Any
from spectacles.validators.validator import Validator
from spectacles.exceptions import LookMLError, ValidationError


class LookMLValidator(Validator):
    """Runs LookML validator for a given project.

    Args:
        client: Looker API client.
        project: Name of the LookML project to validate.

    """

    def validate(self) -> Dict[str, Any]:
        validation_results = self.client.lookml_validation(self.project.name)
        errors = []
        for error in validation_results["errors"]:

            if error["file_path"]:
                lookml_url = (
                    self.client.base_url
                    + "/projects/"
                    + self.project.name
                    + "/files/"
                    + error["file_path"]
                )
                if error["line_number"]:
                    lookml_url += "?line=" + str(error["line_number"])
            else:
                error["file_path"] = "File path not determinable."
                lookml_url = None

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
        result = {
            "validator": "lookml",
            "errors": [error.__dict__ for error in errors],
            "status": "passed" if not errors else "failed",
        }
        return result
