import os
import textwrap
from typing import List, Optional
import colorama  # type: ignore
from spectacles.logger import GLOBAL_LOGGER as logger, log_sql_error, COLORS

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


def yellow(text: str) -> str:
    return color(text, "yellow")


def print_header(
    text: str,
    line_width: int = LINE_WIDTH,
    char: str = "=",
    leading_newline: bool = True,
) -> None:
    header = f" {text} ".center(line_width, char)
    if leading_newline:
        header = "\n" + header
    logger.info(f"{header}\n")


def print_content_error(
    model: str,
    explore: str,
    message: str,
    content_type: str,
    tile_type: Optional[str],
    tile_title: Optional[str],
    space: str,
    title: str,
    url: str,
):
    path = f"{title} [{space}]"
    print_header(red(path), LINE_WIDTH + COLOR_CODE_LENGTH)

    if content_type == "dashboard":
        if tile_type == "dashboard_filter":
            tile_type = "Filter"
        else:
            tile_type = "Tile"
        line = f"{tile_type} '{tile_title}' failed validation."
        wrapped = textwrap.fill(line, LINE_WIDTH)
        logger.info(wrapped + "\n")

    line = f"Error in {model}/{explore}: {message}"
    wrapped = textwrap.fill(line, LINE_WIDTH)
    logger.info(wrapped)

    content_type = content_type.title()
    logger.info("\n" + f"{content_type.title()}: {url}")


def print_data_test_error(
    model: str, explore: str, test_name: str, message: str, lookml_url: str
) -> None:
    path = f"{model}/{explore}/{test_name}"
    print_header(red(path), LINE_WIDTH + COLOR_CODE_LENGTH)
    wrapped = textwrap.fill(message, LINE_WIDTH)
    logger.info(wrapped)
    logger.info("\n" + f"LookML: {lookml_url}")


def print_lookml_error(
    file_path: str, line_number: int, severity: str, message: str, lookml_url: str
) -> None:
    if file_path is None:
        file_path = "[File name not given by Looker]"
    header_color = red if severity in ("fatal", "error") else yellow
    print_header(
        header_color(f"{file_path}:{line_number}"), LINE_WIDTH + COLOR_CODE_LENGTH
    )
    wrapped = textwrap.fill(f"[{severity.title()}] {message}", LINE_WIDTH)
    logger.info(wrapped)
    if lookml_url:
        logger.info("\n" + f"LookML: {lookml_url}")


def print_lookml_success() -> None:
    logger.info(green("✓ No LookML errors found."))


def print_sql_error(
    model: str,
    explore: str,
    message: str,
    sql: str,
    log_dir: str,
    dimension: Optional[str] = None,
    lookml_url: Optional[str] = None,
) -> None:
    path = model + "/"
    if dimension:
        path += dimension
    else:
        path += explore
    print_header(red(path), LINE_WIDTH + COLOR_CODE_LENGTH)
    wrapped = textwrap.fill(message, LINE_WIDTH)
    logger.info(wrapped)

    if lookml_url:
        logger.info("\n" + f"LookML: {lookml_url}")

    file_path = log_sql_error(model, explore, sql, log_dir, dimension)
    logger.info("\n" + f"Test SQL: {file_path}")


def print_validation_result(status: str, source: str):
    bullet = "✗" if status == "failed" else "✓"
    if status == "passed":
        message = green(source)
    elif status == "failed":
        message = red(source)
    elif status == "skipped":
        message = dim(source)
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
