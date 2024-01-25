from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

import pytest

from spectacles import utils
from spectacles.logger import GLOBAL_LOGGER as logger

TEST_BASE_URL = "https://test.looker.com"


def test_compose_url_one_path_component() -> None:
    url = utils.compose_url(TEST_BASE_URL, ["api"])
    assert url == "https://test.looker.com/api"


def test_compose_url_multiple_path_components() -> None:
    url = utils.compose_url(TEST_BASE_URL, ["api", "3.0", "login", "42", "auth", "27"])
    assert url == "https://test.looker.com/api/3.0/login/42/auth/27"


def test_compose_url_multiple_path_components_and_multiple_field_params() -> None:
    url = utils.compose_url(
        TEST_BASE_URL,
        ["api", "3.0", "login", "42", "auth", "27"],
        {"fields": ["joins", "id"]},
    )
    assert url == "https://test.looker.com/api/3.0/login/42/auth/27?fields=joins%2Cid"


def test_compose_url_multiple_path_components_and_one_field_params() -> None:
    url = utils.compose_url(
        TEST_BASE_URL,
        ["api", "3.0", "login", "42", "auth", "27"],
        {"fields": ["joins"]},
    )
    assert url == "https://test.looker.com/api/3.0/login/42/auth/27?fields=joins"


def test_compose_url_with_extra_slashes() -> None:
    url = utils.compose_url(TEST_BASE_URL + "/", ["/api//", "3.0/login/"])
    assert url == "https://test.looker.com/api/3.0/login"


human_readable_testcases = [
    (0.000002345, "0 seconds"),
    (0.02, "0 seconds"),
    (60, "1 minute"),
    (61.002, "1 minute and 1 second"),
    (62, "1 minute and 2 seconds"),
    (2790, "46 minutes and 30 seconds"),
]


@pytest.mark.parametrize("elapsed,expected", human_readable_testcases)
def test_human_readable(elapsed: float, expected: str) -> None:
    human_readable = utils.human_readable(elapsed)
    assert human_readable == expected


get_detail_testcases = [
    ("run_sql", "SQL "),
    ("run_assert", "data test "),
    ("run_content", "content "),
    ("OtherClass.validate", ""),
]


@pytest.mark.parametrize("fn_name,expected", get_detail_testcases)
def test_get_detail(fn_name: str, expected: str) -> None:
    detail = utils.get_detail(fn_name)
    assert detail == expected


class TestLogDurationDecorator(IsolatedAsyncioTestCase):
    async def test_log_SQL(self) -> None:
        with self.assertLogs(logger=logger, level="INFO") as cm:
            func = AsyncMock()
            func.__name__ = "run_sql"
            decorated_func = utils.log_duration(func)
            await decorated_func()
        self.assertIn("INFO:spectacles:Completed SQL validation in", cm.output[0])

    async def test_log_assert(self) -> None:
        with self.assertLogs(logger=logger, level="INFO") as cm:
            func = AsyncMock()
            func.__name__ = "run_assert"
            decorated_func = utils.log_duration(func)
            await decorated_func()
        self.assertIn("INFO:spectacles:Completed data test validation in", cm.output[0])

    async def test_log_content(self) -> None:
        with self.assertLogs(logger=logger, level="INFO") as cm:
            func = AsyncMock()
            func.__name__ = "run_content"
            decorated_func = utils.log_duration(func)
            await decorated_func()
        self.assertIn("INFO:spectacles:Completed content validation in", cm.output[0])

    async def test_log_other(self) -> None:
        with self.assertLogs(logger=logger, level="INFO") as cm:
            func = AsyncMock()
            func.__name__ = "OtherValidator.validate"
            decorated_func = utils.log_duration(func)
            await decorated_func()
        self.assertIn("INFO:spectacles:Completed validation in", cm.output[0])


def test_chunks_returns_expected_results() -> None:
    to_chunk = list(range(10))  # has length of 10
    assert len(list(utils.chunks(to_chunk, 5))) == 2
    assert len(list(utils.chunks(to_chunk, 9))) == 2
    assert len(list(utils.chunks(to_chunk, 10))) == 1
    assert len(list(utils.chunks(to_chunk, 11))) == 1
