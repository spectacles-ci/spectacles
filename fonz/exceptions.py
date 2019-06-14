class FonzException(Exception):
    exit_code = 100


class ConnectionError(FonzException):
    exit_code = 101


class ValidationError(FonzException):
    exit_code = 102


class SqlError(FonzException):
    def __init__(self, query_id, explore_name, message):
        self.query_id = query_id
        self.explore_name = explore_name
        self.message = message

    def __str__(self):
        return repr(self.message)
