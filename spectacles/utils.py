from __future__ import annotations

import asyncio
import hashlib
import time
from typing import Any, Callable, Coroutine, Dict, Iterable, List, Optional, Tuple
from urllib import parse

import httpx

from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.models import T


def compose_url(base_url: str, path: List[str], params: Dict[str, Any] = {}) -> str:
    if not isinstance(path, list):
        raise TypeError("URL path must be a list")
    path_parts = [base_url] + path
    url_with_path = "/".join(str(part).strip("/") for part in path_parts)

    # comma separate each param list
    for k in params.keys():
        params[k] = ",".join(params[k])

    encoded_params = parse.urlencode(params)
    params_parts = [url_with_path, encoded_params]
    url = "?".join(str(part) for part in params_parts).strip("?")

    return url


def details_from_http_error(response: httpx.Response) -> Optional[Dict[str, Any]]:
    try:
        details = response.json()
    # Requests raises a ValueError if the response is invalid JSON
    except ValueError:
        details = None
    return details  # type: ignore[no-any-return]


def human_readable(elapsed: float) -> str:
    minutes, seconds = divmod(elapsed, 60)
    num_mins = f"{minutes:.0f} minute{'s' if minutes > 1 else ''}"
    num_secs = f"{seconds:.0f} second{'s' if round(seconds) != 1 else ''}"
    separator = " and " if seconds and minutes else ""

    return f"{num_mins if minutes else ''}{separator}{num_secs if seconds else ''}"


def get_detail(fn_name: str) -> str:
    detail_map = {
        "run_sql": "SQL ",
        "run_assert": "data test ",
        "run_content": "content ",
    }
    return detail_map.get(fn_name, "")


def log_duration(
    fn: Callable[..., Coroutine[Any, Any, Any]]
) -> Callable[..., Coroutine[Any, Any, Any]]:
    async def timed_function(*args: Any, **kwargs: Any) -> Any:
        start_time = time.time()
        try:
            result = await fn(*args, **kwargs)
        finally:
            elapsed = time.time() - start_time
            elapsed_str = human_readable(int(elapsed))
            message_detail = get_detail(fn.__name__)

            logger.info(f"Completed {message_detail}validation in {elapsed_str}.\n")
        return result

    return timed_function


def time_hash() -> str:
    hash = hashlib.sha1()  # nosec
    hash.update(str(time.time()).encode("utf-8"))
    return hash.hexdigest()[:10]


def chunks(to_chunk: list[T], size: int) -> Iterable[list[T]]:
    """Yield successive n-sized chunks from the list."""
    for i in range(0, len(to_chunk), size):
        yield to_chunk[i : i + size]


def consume_queue(
    queue: asyncio.Queue[T], limit: Optional[int] = None
) -> Tuple[T, ...]:
    """Purge an async queue of all its contents, up to a limit, and return them."""
    count = 0
    contents: tuple[T, ...] = tuple()
    while not queue.empty() and (limit is None or count <= limit):
        contents += (queue.get_nowait(),)
        count += 1
    return contents


def halt_queue(queue: asyncio.Queue[Any]) -> None:
    """Inform a queue that all tasks are finished, unblocking any Queue.join calls."""
    while True:
        try:
            queue.task_done()
        except ValueError:  # ValueError raised when no unfinished tasks remain
            break
