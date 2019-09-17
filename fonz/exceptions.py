class FonzException(Exception):
    exit_code = 100


class ConnectionError(FonzException):
    exit_code = 101


class ValidationError(FonzException):
    exit_code = 102


class QueryNotFinished(FonzException):
    pass


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
        self.sql = sql
        self.line_number = line_number
        self.url = url

    def __repr__(self):
        return self.message
