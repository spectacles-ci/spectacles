from typing import List, Callable
from spectacles.logger import GLOBAL_LOGGER as logger
import functools
import requests
import timeit


def compose_url(base_url: str, path: List) -> str:
    if not isinstance(path, list):
        raise TypeError("URL path must be a list")
    parts = [base_url] + path
    url = "/".join(str(part).strip("/") for part in parts)
    return url


def details_from_http_error(response: requests.Response) -> str:
    try:
        response_json = response.json()
    # Requests raises a ValueError if the response is invalid JSON
    except ValueError:
        details = ""
    else:
        details = response_json.get("message")
    details = details.strip() if details else ""
    return details


def human_readable(elapsed: int):
    minutes, seconds = divmod(elapsed, 60)
    num_minutes = f"{minutes:.0f} minute{'s' if minutes > 1 else ''}{' and ' if seconds else ''}"
    num_seconds = f"{seconds:.0f} second{'s' if round(seconds) != 1 else ''}"

    return(f"{num_minutes if minutes else ''}{num_seconds if seconds else ''}")


def log_time(fn: Callable):
    functools.wraps(fn)
    def timed_function(*args, **kwargs):
        start_time = timeit.default_timer()
        result = fn(*args, **kwargs)
        elapsed = timeit.default_timer() - start_time
        logger.info(
            f"\nCompleted SQL validation in {human_readable(elapsed)}."
            )
        return result
    return timed_function
