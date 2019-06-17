class FonzException(Exception):
    exit_code = 100


class ConnectionError(FonzException):
    exit_code = 101


class ValidationError(FonzException):
    exit_code = 102


class QueryNotFinished(FonzException):
    pass


class SqlError(FonzException):
    def __init__(self, message, sql, line_number):
        self.message = message
        self.sql = sql
        self.line_number = line_number

    def __repr__(self):
        return self.message
