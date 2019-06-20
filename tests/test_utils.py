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


def test_extract_sql_context_line_number_close_to_end():
    text = "\n".join([f"{n}" for n in range(1, 21)])
    expected_result = "| 17\n| 18\n* 19\n| 20"
    result = utils.extract_sql_context(sql=text, line_number=19, window_size=2)
    assert result == expected_result


def test_extract_sql_context_line_number_close_to_beginning():
    text = "\n".join([f"{n}" for n in range(1, 21)])
    expected_result = "| 1\n* 2\n| 3\n| 4"
    result = utils.extract_sql_context(sql=text, line_number=2, window_size=2)
    assert result == expected_result


def test_mark_line_odd_number_of_lines():
    text = [f"{n}" for n in range(1, 6)]
    expected_result = ["| 1", "| 2", "* 3", "| 4", "| 5"]
    result = utils.mark_line(lines=text, line_number=3)
    assert result == expected_result


def test_mark_line_even_number_of_lines():
    text = [f"{n}" for n in range(1, 5)]
    expected_result = ["| 1", "* 2", "| 3", "| 4"]
    result = utils.mark_line(lines=text, line_number=2)
    assert result == expected_result
