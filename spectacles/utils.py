from typing import List, Callable, Optional, Dict, Any
from spectacles.logger import GLOBAL_LOGGER as logger
import functools
import requests
import timeit
import hashlib
import time


def compose_url(base_url: str, path: List) -> str:
    if not isinstance(path, list):
        raise TypeError("URL path must be a list")
    parts = [base_url] + path
    url = "/".join(str(part).strip("/") for part in parts)
    return url


def details_from_http_error(response: requests.Response) -> Optional[Dict[str, Any]]:
    try:
        details = response.json()
    # Requests raises a ValueError if the response is invalid JSON
    except ValueError:
        details = None
    return details


def human_readable(elapsed: int):
    minutes, seconds = divmod(elapsed, 60)
    num_mins = f"{minutes:.0f} minute{'s' if minutes > 1 else ''}"
    num_secs = f"{seconds:.0f} second{'s' if round(seconds) != 1 else ''}"
    separator = " and " if seconds and minutes else ""

    return f"{num_mins if minutes else ''}{separator}{num_secs if seconds else ''}"


def get_detail(fn_name: str):
    detail_map = {"validate_sql": "SQL ", "validate_data_tests": "test "}
    return detail_map.get(fn_name, "")


def log_duration(fn: Callable):
    functools.wraps(fn)

    def timed_function(*args, **kwargs):
        start_time = timeit.default_timer()
        result = fn(*args, **kwargs)
        elapsed = timeit.default_timer() - start_time
        elapsed_str = human_readable(elapsed)
        message_detail = get_detail(fn.__name__)

        logger.info(f"\nCompleted {message_detail}validation in {elapsed_str}.")
        return result

    return timed_function


def time_hash() -> str:
    hash = hashlib.sha1()
    hash.update(str(time.time()).encode("utf-8"))
    return hash.hexdigest()[:10]
