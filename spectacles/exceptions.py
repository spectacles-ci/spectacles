from typing import Dict, Any, Optional
import requests
from spectacles.utils import details_from_http_error
from spectacles.types import JsonDict


class SpectaclesException(Exception):
    exit_code = 100

    def __init__(self, name: str, title: str, detail: str):
        self.type: str = "/errors/" + name
        self.title = title
        self.detail = detail

    def __repr__(self) -> str:
        return self.title

    def __str__(self) -> str:
        return self.title + " " + self.detail


class LookMlNotFound(SpectaclesException):
    ...


class LookerApiError(SpectaclesException):
    """Exception raised when an error is returned by the Looker API.

    Args:
        name: A lowercase, hyphenated, unique ID for the error type.
        title: A short, human-readable summary of the problem.
        status: The HTTP status code returned by the Looker API.
        detail: A human-readable explanation with any helpful tips for
            solving the issue.
        response: The response object returned by the Looker API.
    """

    exit_code = 101

    def __init__(
        self,
        name: str,
        title: str,
        status: int,
        detail: str,
        response: requests.Response,
    ):
        request: requests.PreparedRequest = response.request
        super().__init__("looker-api-errors/" + name, title, detail)
        self.status = status
        self.looker_api_response: Optional[JsonDict] = details_from_http_error(response)
        self.request = {"url": request.url, "method": request.method}


class GenericValidationError(SpectaclesException):
    exit_code = 102

    def __init__(self):
        super().__init__(
            name="validation-error",
            title="A validation error occurred.",
            detail="Spectacles encountered an error while running validation tests.",
        )


class ValidationError(GenericValidationError):
    def __init__(
        self, model: str, explore: str, message: str, metadata: Dict[str, Any]
    ):
        MAX_WORDS = 100
        words = message.split(" ")
        # On some warehouses, these error messages are prohibitively long
        # Truncate to n words to keep the response lightweight
        if len(words) > MAX_WORDS:
            self.message = " ".join(words[:MAX_WORDS]) + "..."
        else:
            self.message = message

        self.model = model
        self.explore = explore
        self.metadata = metadata
        super().__init__()

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
            "sql": sql,
            "line_number": line_number,
            "explore_url": explore_url,
            "lookml_url": lookml_url,
        }
        super().__init__(
            model=model, explore=explore, message=message, metadata=metadata
        )


class DataTestError(ValidationError):
    def __init__(
        self, model: str, explore: str, message: str, test_name: str, lookml_url: str
    ):
        metadata = {"test_name": test_name, "lookml_url": lookml_url}
        super().__init__(
            model=model, explore=explore, message=message, metadata=metadata
        )


class ContentError(ValidationError):
    def __init__(
        self,
        model: str,
        explore: str,
        message: str,
        field_name: str,
        content_type: str,
        title: str,
        space: str,
        url: str,
    ):
        metadata = {
            "field_name": field_name,
            "content_type": content_type,
            "title": title,
            "space": space,
            "url": url,
        }
        super().__init__(
            model=model, explore=explore, message=message, metadata=metadata
        )
