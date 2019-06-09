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

explores = {
    "explore_one": {
        "dimensions": [
            {"name": "dimension_one", "sql": "${TABLE}.dimension_one"},
            {"name": "dimension_two", "sql": "${TABLE}.dimension_two"},
        ],
        "query_id": 1,
    },
    "explore_two": {
        "dimensions": [
            {"name": "dimension_three", "sql": "${TABLE}.dimension_three"},
            {"name": "dimension_four", "sql": "${TABLE}.dimension_four"},
        ],
        "query_id": 2,
    },
    "explore_three": {
        "dimensions": [
            {"name": "dimension_one", "sql": "${TABLE}.dimension_one"},
            {"name": "dimension_two", "sql": "${TABLE}.dimension_two"},
        ],
        "query_id": 3,
    },
    "explore_four": {
        "dimensions": [
            {"name": "dimension_three", "sql": "${TABLE}.dimension_three"},
            {"name": "dimension_four", "sql": "${TABLE}.dimension_four"},
        ],
        "query_id": 4,
    },
}

queries = [
    {"id": 1, "response": [{"column_one": 123}]},
    {"id": 2, "response": [{"column_one": 123}]},
    {"id": 3, "response": []},
    {"id": 4, "response": [{"looker_error": "What went wrong?"}]},
]

looker_mock = requests_mock.Mocker()


# POST login
def login_callback(request, context):
    data = request.json()
    if data["client_id"] == "CLIENT_ID" and data["client_secret"] == "CLIENT_SECRET":
        context.status_code = 200
        return {"access_token": "FAKE_ACCESS_TOKEN"}
    else:
        context.status_code = 404


looker_mock.post(compose_url(BASE_URL_30, path=["login"]), json=login_callback)

# PATCH session
looker_mock.patch(compose_url(BASE_URL_30, path=["session"]))

# PUT git_branch
looker_mock.put(
    compose_url(BASE_URL_30, path=["projects", "test_project", "git_branch"])
)

# POST queries
def create_query_callback(request, context):
    print(request.json())
    data = request.json()
    try:
        query_id = explores[data["view"]]["query_id"]
        context.status_code = 200
        return {"id": query_id}
    except:
        context.status_code = 404


looker_mock.post(compose_url(BASE_URL_30, path=["queries"]), json=create_query_callback)

# GET queries
for query in queries:
    looker_mock.get(
        compose_url(BASE_URL_30, path=["queries", query["id"], "run", "json"]),
        json=query["response"],
    )


# GET lookml_models
looker_mock.get(compose_url(BASE_URL_30, path=["lookml_models"]), json=lookml_models)

# GET explores
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
                "fields": {"dimensions": explores[explore["name"]]["dimensions"]},
            },
        )
