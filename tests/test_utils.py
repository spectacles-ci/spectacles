from constants import TEST_BASE_URL
from fonz import utils


def test_compose_url_one_path_component():
    url = utils.compose_url(TEST_BASE_URL, ["api"])
    assert url == "https://test.looker.com/api"


def test_compose_url_multiple_path_components():
    url = utils.compose_url(TEST_BASE_URL, ["api", "3.0", "login", "42", "auth", "27"])
    assert url == "https://test.looker.com/api/3.0/login/42/auth/27"


def test_compose_url_with_extra_slashes():
    url = utils.compose_url(TEST_BASE_URL + "/", ["/api//", "3.0/login/"])
    assert url == "https://test.looker.com/api/3.0/login"


def test_extract_sql_context():
    text = "\n".join([f"{n}" for n in range(1, 21)])
    expected_result = "| 4\n| 5\n* 6\n| 7\n| 8"
    result = utils.extract_sql_context(sql=text, line_number=6, window_size=2)
    assert result == expected_result
