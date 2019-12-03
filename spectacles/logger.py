import logging
import colorama  # type: ignore


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


logger = logging.getLogger("spectacles")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

logger.addHandler(ch)

GLOBAL_LOGGER = logger
