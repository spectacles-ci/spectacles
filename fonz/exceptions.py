class FonzException(Exception):
    exit_code = 100


class ConnectionError(FonzException):
    exit_code = 101


class ValidationError(FonzException):
    exit_code = 102
