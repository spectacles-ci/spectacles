import pytest
from spectacles.exceptions import LookerApiError
from spectacles.runner import Runner
from typing import Iterable
import vcr
import os


@pytest.fixture(scope="class")
def runner(record_mode) -> Iterable[Runner]:
    with vcr.use_cassette(
        "tests/cassettes/init_runner.yaml",
        filter_post_data_parameters=["client_id", "client_secret"],
        filter_headers=["Authorization"],
        record_mode=record_mode,
    ):
        runner = Runner(
            project="eye_exam",
            branch="pytest",
            base_url="https://spectacles.looker.com",
            client_id=os.environ.get("LOOKER_CLIENT_ID", ""),
            client_secret=os.environ.get("LOOKER_CLIENT_SECRET", ""),
        )
        yield runner


class TestImportProjects:
    def test_validate_sql_with_import_projects_error(self, runner):
        runner.branch_manager.import_projects = True
        with pytest.raises(LookerApiError):
            runner.validate_sql(["*/*"], [])

    def test_validate_assert_with_import_projects_error(self, runner):
        runner.branch_manager.import_projects = True
        with pytest.raises(LookerApiError):
            runner.validate_data_tests()
