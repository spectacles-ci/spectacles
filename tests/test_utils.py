from fonz import utils

TEST_BASE_URL = "https://test.looker.com"


def test_compose_url_one_path_component():
    url = utils.compose_url(TEST_BASE_URL, ["api"])
    assert url == "https://test.looker.com/api"


def test_compose_url_multiple_path_components():
    url = utils.compose_url(TEST_BASE_URL, ["api", "3.0", "login", "42", "auth", "27"])
    assert url == "https://test.looker.com/api/3.0/login/42/auth/27"


def test_compose_url_with_extra_slashes():
    url = utils.compose_url(TEST_BASE_URL + "/", ["/api//", "3.0/login/"])
    assert url == "https://test.looker.com/api/3.0/login"
