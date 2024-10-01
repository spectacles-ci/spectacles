from typing import Any, Dict, Optional

import httpx

from spectacles.models import JsonDict
from spectacles.utils import details_from_http_error


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

    def to_dict(self) -> dict[str, Any]:
        """Returns a dictionary representation, scrubbed of private attributes"""
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}


class LookMlNotFound(SpectaclesException): ...


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
        response: httpx.Response,
    ):
        request: httpx.Request = response.request
        super().__init__("looker-api-errors/" + name, title, detail)
        self.status = status
        self.looker_api_response: Optional[JsonDict] = details_from_http_error(response)
        self.request = {"url": request.url, "method": request.method}


class GenericValidationError(SpectaclesException):
    exit_code = 102

    def __init__(self) -> None:
        super().__init__(
            name="validation-error",
            title="A validation error occurred.",
            detail="Spectacles encountered an error while running validation tests.",
        )


class ValidationError(GenericValidationError):
    def __init__(
        self, model: str, explore: str, message: str, metadata: Dict[str, Any]
    ) -> None:
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
        self._ignore: bool = False
        super().__init__()

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented

        return self.__dict__ == other.__dict__

    def __repr__(self) -> str:
        return self.message

    @property
    def ignore(self) -> bool:
        # Hide this in a property so we can skip it in `to_dict`
        return self._ignore

    @ignore.setter
    def ignore(self, value: bool) -> None:
        self._ignore = value


class LookMLError(ValidationError):
    def __init__(
        self,
        model: str,
        explore: str,
        field_name: str,
        message: str,
        severity: str,
        lookml_url: Optional[str],
        file_path: Optional[str],
        line_number: Optional[int] = None,
    ) -> None:
        metadata = {
            "line_number": line_number,
            "lookml_url": lookml_url,
            "dimension": field_name,
            "file_path": file_path,
            "severity": severity,
        }
        super().__init__(
            model=model, explore=explore, message=message, metadata=metadata
        )


class SqlError(ValidationError):
    def __init__(
        self,
        model: str,
        explore: str,
        dimension: Optional[str],
        sql: Optional[str],
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
        self,
        model: str,
        explore: str,
        message: str,
        test_name: str,
        lookml_url: str,
        explore_url: str,
    ):
        metadata = {
            "test_name": test_name,
            "lookml_url": lookml_url,
            "explore_url": explore_url,
        }
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
        folder: Optional[str],
        url: str,
        tile_type: Optional[str] = None,
        tile_title: Optional[str] = None,
    ):
        metadata = {
            "field_name": field_name,
            "content_type": content_type,
            "title": title,
            "folder": folder,
            "url": url,
        }
        if tile_type and tile_title:
            metadata["tile_type"] = tile_type
            metadata["tile_title"] = tile_title
        super().__init__(
            model=model, explore=explore, message=message, metadata=metadata
        )
