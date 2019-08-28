import os
from typing import Dict, Any, Sequence, List, Optional
import textwrap
from fonz.logger import GLOBAL_LOGGER as logger
import colorama  # type: ignore
import time

JsonDict = Dict[str, Any]

COLORS = {
    "red": colorama.Fore.RED,
    "green": colorama.Fore.GREEN,
    "yellow": colorama.Fore.YELLOW,
    "cyan": colorama.Fore.CYAN,
    "bold": colorama.Style.BRIGHT,
    "dim": colorama.Style.DIM,
}

PRINTER_WIDTH = 80


def get_timestamp() -> str:
    return time.strftime("%H:%M:%S")


def color(text: Optional[str], name: str) -> str:
    if os.environ.get("NO_COLOR") or os.environ.get("TERM") == "dumb":
        return str(text)
    else:
        return f"{COLORS[name]}{text}{colorama.Style.RESET_ALL}"


def print_header(msg: str) -> None:
    header = f" {msg} ".center(PRINTER_WIDTH, "=")
    logger.info(f"\n{header}\n")


def mark_line(lines: Sequence, line_number: int, char: str = "*") -> List:
    """For a list of strings, mark a specified line with a prepended character."""
    line_number -= 1  # Align with array indexing
    marked = []
    for i, line in enumerate(lines):
        if i == line_number:
            marked.append(char + " " + line)
        else:
            marked.append("| " + line)
    return marked


def extract_sql_context(sql: str, line_number: int, window_size: int = 2) -> str:
    """Extract a line of SQL with a specified amount of surrounding context."""
    split = sql.split("\n")
    line_number -= 1  # Align with array indexing
    line_start = line_number - window_size
    line_end = line_number + (window_size + 1)
    line_start = line_start if line_start >= 0 else 0
    line_end = line_end if line_end <= len(split) else len(split)

    selected_lines = split[line_start:line_end]
    marked = mark_line(selected_lines, line_number=line_number - line_start + 1)
    context = "\n".join(marked)
    return context


def print_sql_error(path, msg, sql, line_number, *footers) -> None:
    adjusted_width = PRINTER_WIDTH + 2  # Account for two color characters for bold
    wrapped = textwrap.fill(f"Error in {path}: {color(msg, 'bold')}")
    sql_context = extract_sql_context(sql, line_number)
    print_error(wrapped + "\n")
    logger.info(sql_context + "\n")
    for footer in footers:
        logger.info(footer)
    logger.info("")


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
    print_fancy_line(msg, color("PASS", "green"), index, total)


def print_fail(explore_name: str, index: int, total: int) -> None:
    msg = f"FAILED explore: {explore_name}"
    print_fancy_line(msg, color("FAIL", "red"), index, total)


def print_error(message: str) -> None:
    logger.info(color(message, "red"))


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
