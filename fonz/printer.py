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


def yellow(text):
    return color(text, COLOR_FG_YELLOW)


def print_fancy_line(msg: str, status: str, index: int, total: int) -> None:
    progress = "{} of {} ".format(index, total)
    prefix = "{timestamp} | {progress}{message}".format(
        timestamp=get_timestamp(), progress=progress, message=msg
    )

    justified = prefix.ljust(PRINTER_WIDTH, ".")

    status_txt = status

    output = "{justified} [{status}]".format(justified=justified, status=status_txt)

    logger.info(output)


def print_start(explore_name: str, index: int, total: int) -> None:
    msg = f"CHECKING explore: {explore_name}"
    print_fancy_line(msg, "START", index, total)


def print_pass(explore_name: str, index: int, total: int) -> None:
    msg = f"PASSED explore: {explore_name}"
    print_fancy_line(msg, green("PASS"), index, total)


def print_fail(explore_name: str, index: int, total: int) -> None:
    msg = f"FAILED explore: {explore_name}"
    print_fancy_line(msg, red("FAIL"), index, total)


def print_error(message: str):
    logger.info(yellow("\n" + message))


def print_stats(errors: int, total: int) -> None:
    stats = {"error": errors, "pass": total - errors, "total": total}

    stats_line = "\nDone. PASS={pass} ERROR={error} TOTAL={total}"
    logger.info(stats_line.format(**stats))


def print_progress(
    iteration: int,
    total: int,
    prefix: str = "",
    suffix: str = "",
    decimals: int = 1,
    length: int = 80,
    fill: str = "█",
):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percentage
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))

    filledLength = int(length * iteration // total)
    bar = fill * filledLength + "-" * (length - filledLength)
    print("\r%s |%s| %s%% %s" % (prefix, bar, percent, suffix), end="\r")
    # Print New Line on Complete
    if iteration == total:
        print("\n")
