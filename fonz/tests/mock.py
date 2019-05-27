import pytest
import requests
import requests_mock
from fonz.utils import compose_url


base_30 = 'https://test.looker.com:19999/api/3.0/'

lookml_models = [
    {"name": "model_one", "project_name": "test_project",
     "explores": [{"name": "explore_one"}, {"name": "explore_two"}]},
    {"name": "model_two", "project_name": "not_test_project",
     "explores": [{"name": "explore_three"}, {"name": "explore_four"}]}
]

dimensions = {
    'explore_one': [{'name': 'dimension_one'}, {'name': 'dimension_two'}],
    'explore_two': [{'name': 'dimension_three'}, {'name': 'dimension_four'}],
    'explore_three': [{'name': 'dimension_one'}, {'name': 'dimension_two'}],
    'explore_four': [{'name': 'dimension_three'}, {'name': 'dimension_four'}]
}

looker_mock = requests_mock.Mocker()

looker_mock.get(
    url=compose_url(base_30, 'lookml_models'),
    json=lookml_models
        )

for model in lookml_models:
    for explore in model['explores']:
        looker_mock.get(
            url=compose_url(
                base_30,
                'lookml_models',
                model['name'],
                'explores',
                explore['name']),
            json={
                'name': explore['name'],
                'fields': {
                     'dimensions': dimensions[explore['name']]
                }
            }
            )
