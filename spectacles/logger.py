import logging
import textwrap
from pathlib import Path
from typing import Optional, cast

import colorama

LOG_FILENAME = "spectacles.log"
COLORS = {
    "red": colorama.Fore.RED,
    "green": colorama.Fore.GREEN,
    "yellow": colorama.Fore.YELLOW,
    "cyan": colorama.Fore.CYAN,
    "bold": colorama.Style.BRIGHT,
    "dim": colorama.Style.DIM,
    "reset": colorama.Style.RESET_ALL,
}


class IndentedFormatter(logging.Formatter):
    def __init__(self, fmt: Optional[str] = None):
        super().__init__(fmt)
        self._indent = 0

    def indent(self, amount: int) -> None:
        self._indent += amount

    @property
    def _indent_string(self) -> str:
        if self._indent > 0:
            return (".." * self._indent) + " "
        else:
            return ""

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record=record)
        # Only wrap for debug logs, info+ logs can be wrapped in the record itself
        if record.levelno == logging.DEBUG and not record.exc_info:
            return "\n".join(
                textwrap.wrap(
                    self._indent_string + message,
                    width=88,
                    replace_whitespace=False,
                    subsequent_indent=" " + "  " * (self._indent + 1),
                )
            )
        else:
            return message


class FileFormatter(IndentedFormatter):
    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record=record)
        formatted = delete_color_codes(message)
        return formatted


class IndentedLogger(logging.Logger):
    def indent(self, amount: int) -> None:
        for handler in self.handlers:
            if isinstance(handler.formatter, IndentedFormatter):
                handler.formatter.indent(amount)


class BackoffFilter(logging.Filter):
    """Force all logs from the backoff package to be emitted at DEBUG level."""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.DEBUG:
            record.levelno = logging.DEBUG
        return True


logging.setLoggerClass(IndentedLogger)
logger = cast(IndentedLogger, logging.getLogger("spectacles"))
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setFormatter(IndentedFormatter())
ch.setLevel(logging.INFO)

logger.addHandler(ch)

GLOBAL_LOGGER = logger

logging.getLogger("backoff").addFilter(BackoffFilter())


def set_file_handler(log_dir: str) -> None:
    log_dir_path = Path(log_dir)
    LOG_FILEPATH = log_dir_path / LOG_FILENAME
    log_dir_path.mkdir(exist_ok=True)

    # Create subfolder to save the SQL for failed queries
    (log_dir_path / "queries").mkdir(exist_ok=True)

    fh = logging.FileHandler(LOG_FILEPATH, encoding="utf-8")
    fh.setLevel(logging.DEBUG)

    formatter = FileFormatter("%(asctime)s %(levelname)s | %(message)s")
    fh.setFormatter(formatter)

    logger.addHandler(fh)


def delete_color_codes(text: str) -> str:
    for escape_sequence in COLORS.values():
        text = text.replace(escape_sequence, "")
    return text


def log_sql_error(
    model: str, explore: str, sql: str, log_dir: str, dimension: Optional[str] = None
) -> Path:
    file_name = (
        model + "__" + explore + ("__" + dimension if dimension else "")
    ).replace(".", "_")
    file_name += ".sql"
    file_path = Path(log_dir) / "queries" / file_name

    logger.debug(f"Logging failing SQL query to '{file_path}'")
    logger.debug(f"Failing SQL: \n{sql}")

    with open(file_path, "w") as file:
        file.write(sql)

    return file_path
