import pytest
import requests
import requests_mock
from fonz import connection

base = 'https://test.looker.com'


def test_get_explores():

    client = connection.Fonz(
        url=base,
        client_id='client_id',
        client_secret='client_secret',
        port=19999,
        api='3.0',
        project='test_project')

    response = """[
        {
        "name": "model_one",
        "project_name": "test_project",
        "explores": [{"name": "explore_one"}, {"name": "explore_two"}]
        }
    ]"""

    output = [
        {'model': 'model_one', 'explore': 'explore_one'},
        {'model': 'model_one', 'explore': 'explore_two'}
        ]

    with requests_mock.mock() as m:
        m.get(
            url="{}{}".format(client.url, 'lookml_models'),
            text=response
            )

        explores = client.get_explores()

        assert explores == output
