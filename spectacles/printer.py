import os
import textwrap
from typing import List
import colorama  # type: ignore
from spectacles.logger import GLOBAL_LOGGER as logger, COLORS

LINE_WIDTH = 80
COLOR_CODE_LENGTH = len(colorama.Fore.RED) + len(colorama.Style.RESET_ALL)


def color(text: str, name: str) -> str:
    if os.environ.get("NO_COLOR") or os.environ.get("TERM") == "dumb":
        return str(text)
    else:
        return f"{COLORS[name]}{text}{COLORS['reset']}"


def bold(text: str) -> str:
    return color(text, "bold")


def dim(text: str) -> str:
    return color(text, "dim")


def red(text: str) -> str:
    return color(text, "red")


def green(text: str) -> str:
    return color(text, "green")


def print_header(text: str, line_width: int = LINE_WIDTH) -> None:
    header = f" {text} ".center(line_width, "=")
    logger.info(f"\n{header}\n")


def print_data_test_error(error: dict) -> None:
    print_header(red(error["path"]), LINE_WIDTH + COLOR_CODE_LENGTH)
    wrapped = textwrap.fill(error["message"], LINE_WIDTH)
    logger.info(wrapped)


def print_sql_error(error: dict) -> None:
    print_header(red(error["path"]), LINE_WIDTH + COLOR_CODE_LENGTH)
    wrapped = textwrap.fill(error["message"], LINE_WIDTH)
    logger.info(wrapped)
    # if error["line_number"]:
    #     sql_context = extract_sql_context(error["sql"], error["line_number"])
    #     logger.info("\n" + sql_context)
    if error["url"]:
        logger.info("\n" + f"LookML: {error['url']}")


def print_validation_result(type: str, source: str):
    bullet = "✓" if type == "success" else "✗"
    message = green(source) if type == "success" else red(source)
    status = "passed" if type == "success" else "failed"
    logger.info(f"{bullet} {message} {status}")


def mark_line(lines: List[str], line_number: int, char: str = "*") -> List[str]:
    """For a list of strings, mark a specified line with a prepended character."""
    line_number -= 1  # Align with array indexing
    marked = []
    for i, line in enumerate(lines):
        if i == line_number:
            marked.append(char + " " + line)
        else:
            marked.append(dim("| " + line))
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
