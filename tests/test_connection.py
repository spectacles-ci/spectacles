import pytest
import requests
import requests_mock
from tests.mock import looker_mock
from fonz import connection

base = "https://test.looker.com"

client = connection.Fonz(
    url=base,
    client_id="CLIENT_ID",
    client_secret="CLIENT_SECRET",
    port=19999,
    api="3.0",
    project="test_project",
)


def test_connect_correct_credentials():

    client = connection.Fonz(
        url=base,
        client_id="CLIENT_ID",
        client_secret="CLIENT_SECRET",
        port=19999,
        api="3.0",
        project="test_project",
    )

    with looker_mock as m:
        client.connect()
        assert client.session.headers == {"Authorization": "token FAKE_ACCESS_TOKEN"}


def test_connect_incorrect_credentials():

    client = connection.Fonz(
        url=base,
        client_id="CLIENT_ID",
        client_secret="WRONG_CLIENT_SECRET",
        port=19999,
        api="3.0",
        project="test_project",
    )

    with looker_mock as m:
        with pytest.raises(requests.exceptions.HTTPError):
            client.connect()


def test_get_explores():

    client = connection.Fonz(
        url=base,
        client_id="CLIENT_ID",
        client_secret="CLIENT_SECRET",
        port=19999,
        api="3.0",
        project="test_project",
    )

    output = [
        {"model": "model_one", "explore": "explore_one"},
        {"model": "model_one", "explore": "explore_two"},
    ]

    with looker_mock as m:
        response = client.get_explores()
        assert response == output


def test_get_dimensions():

    client = connection.Fonz(
        url=base,
        client_id="CLIENT_ID",
        client_secret="CLIENT_SECRET",
        port=19999,
        api="3.0",
        project="test_project",
    )

    output = ["dimension_one", "dimension_two"]

    with looker_mock as m:
        response = client.get_dimensions("model_one", "explore_one")
        assert response == output
