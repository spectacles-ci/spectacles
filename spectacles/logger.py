from typing import Optional, List
from pathlib import Path
import logging
import colorama  # type: ignore
from spectacles.exceptions import SqlError

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

logger = logging.getLogger("spectacles")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

logger.addHandler(ch)

GLOBAL_LOGGER = logger


def set_file_handler(log_dir: str) -> None:
    log_dir_path = Path(log_dir)
    LOG_FILEPATH = log_dir_path / LOG_FILENAME
    log_dir_path.mkdir(exist_ok=True)

    # Create subfolder to save the SQL for failed queries
    (log_dir_path / "queries").mkdir(exist_ok=True)

    fh = logging.FileHandler(LOG_FILEPATH)
    fh.setLevel(logging.DEBUG)

    formatter = FileFormatter("%(asctime)s %(levelname)s | %(message)s")
    fh.setFormatter(formatter)

    logger.addHandler(fh)


def delete_color_codes(text: str) -> str:
    for escape_sequence in COLORS.values():
        text = text.replace(escape_sequence, "")
    return text


class FileFormatter(logging.Formatter):
    def format(self, record):
        message = super().format(record=record)
        formatted = delete_color_codes(message)
        return formatted


def log_sql_error(
    errors: List[SqlError],
    log_dir: str,
    model_name: str,
    explore_name: str,
    dimension_name: Optional[str] = None,
) -> Path:

    file_name = (
        model_name
        + "__"
        + explore_name
        + ("__" + dimension_name if dimension_name else "")
        + ".sql"
    )
    file_path = Path(log_dir) / "queries" / file_name

    error = errors[0]

    logger.debug(f"Logging failing SQL query for '{error.path}' to '{file_path}'")
    logger.debug(f"Failing SQL for {error.path}: \n{error.sql}")

    with open(file_path, "w") as file:
        file.write(error.sql)

    return file_path
