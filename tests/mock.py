import pytest
import ast
import requests
import requests_mock
from fonz.utils import compose_url
from tests.constants import TEST_BASE_URL
from urllib.parse import parse_qs

BASE_URL_30 = compose_url(TEST_BASE_URL + ":19999", path=["api", "3.0"])

lookml_models = [
    {
        "name": "model_one",
        "project_name": "test_project",
        "explores": [{"name": "explore_one"}, {"name": "explore_two"}],
    },
    {
        "name": "model_two",
        "project_name": "not_test_project",
        "explores": [{"name": "explore_three"}, {"name": "explore_four"}],
    },
]

dimensions = {
    "explore_one": [
        {"name": "dimension_one", "sql": "${TABLE}.dimension_one"},
        {"name": "dimension_two", "sql": "${TABLE}.dimension_two"},
    ],
    "explore_two": [
        {"name": "dimension_three", "sql": "${TABLE}.dimension_three"},
        {"name": "dimension_four", "sql": "${TABLE}.dimension_four"},
    ],
    "explore_three": [
        {"name": "dimension_one", "sql": "${TABLE}.dimension_one"},
        {"name": "dimension_two", "sql": "${TABLE}.dimension_two"},
    ],
    "explore_four": [
        {"name": "dimension_three", "sql": "${TABLE}.dimension_three"},
        {"name": "dimension_four", "sql": "${TABLE}.dimension_four"},
    ],
}

looker_mock = requests_mock.Mocker()


# POST login
def login_callback(request, context):
    print(parse_qs(request.text))
    data = parse_qs(request.text)
    if (
        data["client_id"][0] == "CLIENT_ID"
        and data["client_secret"][0] == "CLIENT_SECRET"
    ):
        context.status_code = 200
        return {"access_token": "FAKE_ACCESS_TOKEN"}
    else:
        context.status_code = 404


looker_mock.post(compose_url(BASE_URL_30, path=["login"]), json=login_callback)

# GET lookml_models
looker_mock.get(compose_url(BASE_URL_30, path=["lookml_models"]), json=lookml_models)

for model in lookml_models:
    for explore in model["explores"]:
        url = compose_url(
            BASE_URL_30,
            path=["lookml_models", model["name"], "explores", explore["name"]],
        )
        looker_mock.get(
            url,
            json={
                "name": explore["name"],
                "fields": {"dimensions": dimensions[explore["name"]]},
            },
        )
