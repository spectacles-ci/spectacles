from typing import Dict, Any, Optional


class SpectaclesException(Exception):
    exit_code = 100


class LookerApiError(SpectaclesException):
    """Exception raised when an error is returned by the Looker API.

    Args:
        name: A lowercase, hyphenated, unique ID for the error type.
        title: A short, human-readable summary of the problem.
        status: The HTTP status code returned by the Looker API.
        detail: A human-readable explanation with any helpful tips for
            solving the issue.
        looker_message: Any useful message returned from the Looker API.
    """

    exit_code = 101

    def __init__(
        self,
        name: str,
        title: str,
        status: int,
        detail: str,
        looker_message: Optional[str],
    ):
        self.name = name
        self.title = title
        self.status = status
        self.detail = detail
        self.looker_message = looker_message

    def __repr__(self):
        return self.title

    def __str__(self):
        return self.title + " " + self.detail


class GenericValidationError(SpectaclesException):
    exit_code = 102


class ValidationError(GenericValidationError):
    def __init__(
        self,
        model: str,
        explore: str,
        test: Optional[str],
        message: str,
        metadata: Dict[str, Any],
    ):
        self.model = model
        self.explore = explore
        self.test = test
        self.message = message
        self.metadata = metadata

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented

        return self.__dict__ == other.__dict__

    def __repr__(self):
        return self.message


class SqlError(ValidationError):
    def __init__(
        self,
        model: str,
        explore: str,
        dimension: Optional[str],
        sql: str,
        message: str,
        line_number: Optional[int] = None,
        explore_url: Optional[str] = None,
        lookml_url: Optional[str] = None,
    ):
        metadata = {
            "dimension": dimension,
            "line_number": line_number,
            "explore_url": explore_url,
            "lookml_url": lookml_url,
        }
        super().__init__(model, explore, sql, message, metadata)


class DataTestError(ValidationError):
    def __init__(self, model: str, explore: str, message: str, test_name: str):
        metadata = {"test_name": test_name}
        super().__init__(model, explore, None, message, metadata)
