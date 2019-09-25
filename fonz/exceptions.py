class FonzException(Exception):
    exit_code = 100


class ApiConnectionError(FonzException):
    exit_code = 101


class ValidationError(FonzException):
    exit_code = 102


class SqlError(ValidationError):
    def __init__(
        self,
        path: str,
        message: str,
        sql: str,
        line_number: int = None,
        url: str = None,
    ):
        super().__init__(message)
        self.path = path
        self.message = message
        self.sql = sql
        self.line_number = line_number
        self.url = url

    def __repr__(self):
        return self.message
