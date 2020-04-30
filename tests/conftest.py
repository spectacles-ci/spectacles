from typing import Dict, Any
import os
import vcr
import pytest
from spectacles.client import LookerClient


@pytest.fixture
def looker_client(scope="session") -> LookerClient:
    with vcr.use_cassette(
        "tests/cassettes/init_client.yaml",
        filter_post_data_parameters=["client_id", "client_secret"],
        filter_headers=["Authorization"],
    ):
        client = LookerClient(
            base_url=os.environ.get("LOOKER_BASE_URL", ""),
            client_id=os.environ.get("LOOKER_CLIENT_ID", ""),
            client_secret=os.environ.get("LOOKER_CLIENT_SECRET", ""),
        )
        client.update_session(
            project="eye_exam", branch="feature/vcr", remote_reset=False
        )
        return client


@pytest.fixture(scope="session")
def vcr_config() -> Dict[str, Any]:
    return {
        # Replace the Authorization request header with "" in cassettes
        "filter_headers": [("Authorization", "")]
    }
