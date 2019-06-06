import pytest
import requests
import requests_mock
from tests import mock
from fonz import connection

base = "https://test.looker.com"

client = connection.Fonz(
    url=base,
    client_id="client_id",
    client_secret="client_secret",
    port=19999,
    api="3.0",
    project="test_project",
)


def test_get_explores():

    output = [
        {"model": "model_one", "explore": "explore_one"},
        {"model": "model_one", "explore": "explore_two"},
    ]

    with mock.looker_mock as m:

        response = client.get_explores()
        assert response == output


def test_get_explore_dimensions():

    body = {"model": "model_one", "explore": "explore_one"}
    output = ["dimension_one", "dimension_two"]

    with mock.looker_mock as m:

        response = client.get_explore_dimensions(body)
        assert response == output


def test_get_dimensions():

    explores = [
        {"model": "model_one", "explore": "explore_one"},
        {"model": "model_one", "explore": "explore_two"},
    ]

    output = [
        {
            "model": "model_one",
            "explore": "explore_one",
            "dimensions": ["dimension_one", "dimension_two"],
        },
        {
            "model": "model_one",
            "explore": "explore_two",
            "dimensions": ["dimension_three", "dimension_four"],
        },
    ]

    with mock.looker_mock as m:

        response = client.get_dimensions(explores)
        print(response)
        assert response == output
