class FonzException(Exception):
    exit_code = 100


class ConnectionError(FonzException):
    exit_code = 101


class ValidationError(FonzException):
    exit_code = 102


class SqlError(FonzException):
    def __init__(self, query_id, message, url=None):
        self.query_id = query_id
        self.message = message
        self.url = url

    def __str__(self):
        return repr(self.message)
