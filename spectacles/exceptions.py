from typing import Dict, Any, Optional


class SpectaclesException(Exception):
    exit_code = 100


class ApiConnectionError(SpectaclesException):
    exit_code = 101


class ValidationError(SpectaclesException):
    exit_code = 102

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
