import pytest
from fonz import utils

base = 'https://test.looker.com/api/3.0/'
endpoint = 'login'
endpoint_id_int = 42
endpoint_id_str = '42'
subendpoint = 'auth'
subendpoint_id_int = 27
subendpoint_id_str = '27'


def test_compose_url_endpoint():
    url = utils.compose_url(base, endpoint)
    assert url == 'https://test.looker.com/api/3.0/login'


def test_compose_url_endpoint_id_str():
    url = utils.compose_url(base, endpoint, endpoint_id_str)
    assert url == 'https://test.looker.com/api/3.0/login/42'


def test_compose_url_endpoint_id_int():
    url = utils.compose_url(base, endpoint, endpoint_id_int)
    assert url == 'https://test.looker.com/api/3.0/login/42'


def test_compose_url_subendpoint():
    url = utils.compose_url(base, endpoint, endpoint_id_str, subendpoint)
    assert url == 'https://test.looker.com/api/3.0/login/42/auth'


def test_compose_url_subendpoint_id_str():
    url = utils.compose_url(
        base, endpoint, endpoint_id_str,
        subendpoint, subendpoint_id_str)
    assert url == 'https://test.looker.com/api/3.0/login/42/auth/27'


def test_compose_url_subendpoint_id_int():
    url = utils.compose_url(
        base, endpoint, endpoint_id_int,
        subendpoint, subendpoint_id_int)
    assert url == 'https://test.looker.com/api/3.0/login/42/auth/27'


def test_compose_url_no_endpoint():
    with pytest.raises(TypeError) as e:
        utils.compose_url(base)
    # assert str(e) == "compose_url requires url_base and endpoint."


def test_compose_url_no_base():
    with pytest.raises(TypeError) as e:
        utils.compose_url(endpoint=endpoint)
    # assert str(e) == "compose_url requires url_base and endpoint."
