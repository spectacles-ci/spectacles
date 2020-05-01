from typing import Iterable
import os
import json
import vcr
import pytest
from spectacles.client import LookerClient


def scrub_access_token(response):
    body = json.loads(response["body"]["string"].decode())
    body["access_token"] = ""
    response["body"]["string"] = json.dumps(body).encode()
    return response


@pytest.fixture(scope="session")
def vcr_config():
    return {"filter_headers": ["Authorization"]}


@pytest.fixture(scope="session")
def looker_client(record_mode) -> Iterable[LookerClient]:
    with vcr.use_cassette(
        "tests/cassettes/init_client.yaml",
        filter_post_data_parameters=["client_id", "client_secret"],
        filter_headers=["Authorization"],
        before_record_response=scrub_access_token,
        record_mode=record_mode,
    ):
        client = LookerClient(
            base_url="https://spectacles.looker.com",
            client_id=os.environ.get("LOOKER_CLIENT_ID", ""),
            client_secret=os.environ.get("LOOKER_CLIENT_SECRET", ""),
        )
        yield client
