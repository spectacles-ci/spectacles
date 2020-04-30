import os
import pytest
from spectacles.client import LookerClient
from spectacles.exceptions import SpectaclesException


@pytest.mark.vcr()
def test_get_looker_release_version(looker_client):
    version = looker_client.get_looker_release_version()
    assert version == "7.4.27"


@pytest.mark.vcr()
def test_unsupported_api_version_raises_error():
    with pytest.raises(SpectaclesException):
        LookerClient(
            base_url=os.environ.get("LOOKER_BASE_URL"),
            client_id=os.environ.get("LOOKER_CLIENT_ID"),
            client_secret=os.environ.get("LOOKER_CLIENT_SECRET"),
            api_version=3.0,
        )


@pytest.mark.vcr()
def test_create_query(looker_client):
    query = looker_client.create_query(
        model="eye_exam", explore="users", dimensions=["id", "age"]
    )
    assert set(("id", "share_url")) <= set(query.keys())
    assert int(query["limit"]) == 0
    assert query["filter_expression"] == "1=2"
    assert query["model"] == "eye_exam"
