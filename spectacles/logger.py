from pathlib import Path
import logging
import colorama  # type: ignore

LOG_DIRECTORY = Path("logs")
LOG_FILEPATH = Path(LOG_DIRECTORY / "logs.txt")

COLORS = {
    "red": colorama.Fore.RED,
    "green": colorama.Fore.GREEN,
    "yellow": colorama.Fore.YELLOW,
    "cyan": colorama.Fore.CYAN,
    "bold": colorama.Style.BRIGHT,
    "dim": colorama.Style.DIM,
    "reset": colorama.Style.RESET_ALL,
}


def delete_color_codes(text: str) -> str:
    for escape_sequence in COLORS.values():
        text = text.replace(escape_sequence, "")
    return text


class FileFormatter(logging.Formatter):
    def format(self, record):
        message = super().format(record=record)
        formatted = delete_color_codes(message)
        return formatted


LOG_DIRECTORY.mkdir(exist_ok=True)

logger = logging.getLogger("spectacles")
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(LOG_FILEPATH)
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = FileFormatter("%(asctime)s %(levelname)s | %(message)s")
fh.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)

GLOBAL_LOGGER = logger
