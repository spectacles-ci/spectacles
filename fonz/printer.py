from typing import Dict, Any
from fonz.logger import GLOBAL_LOGGER as logger
import colorama  # type: ignore
import time

JsonDict = Dict[str, Any]

COLOR_FG_RED = colorama.Fore.RED
COLOR_FG_GREEN = colorama.Fore.GREEN
COLOR_FG_YELLOW = colorama.Fore.YELLOW
COLOR_RESET_ALL = colorama.Style.RESET_ALL

PRINTER_WIDTH = 80


def get_timestamp() -> str:
    return time.strftime("%H:%M:%S")


def color(text: str, color_code: str) -> str:
    return "{}{}{}".format(color_code, text, COLOR_RESET_ALL)


def green(text):
    return color(text, COLOR_FG_GREEN)


def red(text):
    return color(text, COLOR_FG_RED)


def print_fancy_line(msg: str, status: str, index: int, total: int) -> None:
    progress = '{} of {} '.format(index, total)
    prefix = "{timestamp} | {progress}{message}".format(
        timestamp=get_timestamp(),
        progress=progress,
        message=msg)

    justified = prefix.ljust(PRINTER_WIDTH, ".")

    status_txt = status

    output = "{justified} [{status}]".format(
        justified=justified, status=status_txt)

    logger.info(output)


def print_start(explore: JsonDict, index: int, total: int) -> None:
    msg = "CHECKING explore: {}".format(explore['explore'])
    print_fancy_line(msg, 'START', index, total)


def print_pass(explore: JsonDict, index: int, total: int) -> None:
    msg = "PASSED explore: {}".format(explore['explore'])
    print_fancy_line(msg, green('PASS'), index, total)


def print_fail(explore: JsonDict, index: int, total: int) -> None:
    msg = "FAILED explore: {}".format(explore['explore'])
    print_fancy_line(msg, red('FAIL'), index, total)
