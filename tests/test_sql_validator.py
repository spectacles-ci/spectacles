from typing import Iterable, List
from unittest.mock import patch, create_autospec
import pytest
import vcr
from spectacles.validators import SqlValidator
from spectacles.validators.sql import SqlTest, Query
from spectacles.exceptions import SpectaclesException
from spectacles.lookml import Project, build_project


@pytest.fixture(scope="class")
def validator(looker_client, record_mode) -> Iterable[SqlValidator]:
    with vcr.use_cassette(
        "tests/cassettes/test_sql_validator/fixture_validator_init.yaml",
        match_on=["uri", "method", "raw_body"],
        filter_headers=["Authorization"],
        record_mode=record_mode,
    ):
        validator = SqlValidator(looker_client)
        yield validator


class TestValidatePass:
    """Test the eye_exam Looker project on master for an explore without errors."""

    @pytest.fixture(scope="class")
    def project(self, looker_client, record_mode) -> Iterable[Project]:
        with vcr.use_cassette(
            "tests/cassettes/test_sql_validator/fixture_project_pass.yaml",
            match_on=["uri", "method", "raw_body"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            project = build_project(
                looker_client,
                name="eye_exam",
                filters=["eye_exam/users"],
                include_dimensions=True,
            )
            yield project

    @pytest.fixture(scope="class")
    def explore_tests(self, validator, project, record_mode) -> Iterable[List[SqlTest]]:
        with vcr.use_cassette(
            "tests/cassettes/test_sql_validator/fixture_explore_tests_pass.yaml",
            match_on=["uri", "method", "raw_body"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            yield validator.create_tests(project, compile_sql=True)

    @pytest.fixture(scope="class")
    def validator_after_run(
        self, validator, explore_tests, record_mode
    ) -> Iterable[SqlValidator]:
        with vcr.use_cassette(
            "tests/cassettes/test_sql_validator/fixture_validator_pass.yaml",
            match_on=["uri", "method", "raw_body"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            validator.run_tests(list(explore_tests))
            yield validator

    @pytest.fixture(scope="class")
    def dimension_tests(
        self, validator_after_run, project, record_mode
    ) -> Iterable[List[SqlTest]]:
        """Create dimension-level tests after the explore-level validation completes."""
        with vcr.use_cassette(
            "tests/cassettes/test_sql_validator/fixture_dimension_tests_pass.yaml",
            match_on=["uri", "method", "raw_body"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            yield validator_after_run.create_tests(
                project, compile_sql=True, at_dimension_level=True
            )

    def test_project_should_be_queried_but_not_have_errors(
        self, validator_after_run, explore_tests, project
    ):
        assert project.errored is False
        assert project.queried is True
        assert all(test.status != "error" for test in explore_tests)

    def test_running_tests_should_be_empty(self, validator_after_run):
        assert len(validator_after_run._test_by_task_id) == 0

    def test_ignored_dimensions_should_not_be_queried(
        self, validator_after_run, project
    ):
        explore = project.models[0].explores[0]
        assert not any(dim.queried for dim in explore.dimensions if dim.ignore is True)

    def test_should_be_one_explore_test_per_explore(self, explore_tests, project):
        assert project.count_explores() == len(explore_tests)

    def test_should_not_be_any_dimension_tests(self, dimension_tests):
        """We only generate dimension-level tests if the explores have errors."""
        assert len(dimension_tests) == 0

    def test_all_tests_should_be_unique(self, explore_tests):
        assert len(set(explore_tests)) == len(explore_tests)


class TestValidateFail:
    """Test the eye_exam Looker project on master for an explore with errors."""

    @pytest.fixture(scope="class")
    def project(self, looker_client, record_mode) -> Iterable[Project]:
        with vcr.use_cassette(
            "tests/cassettes/test_sql_validator/fixture_project_fail.yaml",
            match_on=["uri", "method", "raw_body"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            project = build_project(
                looker_client,
                name="eye_exam",
                filters=["eye_exam/users__fail"],
                include_dimensions=True,
            )
            yield project

    @pytest.fixture(scope="class")
    def explore_tests(self, validator, project, record_mode) -> Iterable[List[SqlTest]]:
        with vcr.use_cassette(
            "tests/cassettes/test_sql_validator/fixture_explore_tests_fail.yaml",
            match_on=["uri", "method", "raw_body"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            yield validator.create_tests(project, compile_sql=True)

    @pytest.fixture(scope="class")
    def validator_after_run(
        self, validator, explore_tests, record_mode
    ) -> Iterable[SqlValidator]:
        with vcr.use_cassette(
            "tests/cassettes/test_sql_validator/fixture_validator_fail.yaml",
            match_on=["uri", "method", "raw_body"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            validator.run_tests(list(explore_tests))
            yield validator

    @pytest.fixture(scope="class")
    def dimension_tests(
        self, validator_after_run, project, record_mode
    ) -> Iterable[List[SqlTest]]:
        """Create dimension-level tests after the explore-level validation completes."""
        with vcr.use_cassette(
            "tests/cassettes/test_sql_validator/fixture_dimension_tests_fail.yaml",
            match_on=["uri", "method", "raw_body"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            yield validator_after_run.create_tests(
                project, compile_sql=True, at_dimension_level=True
            )

    def test_project_should_be_queried_and_have_at_least_one_error(
        self, validator_after_run, explore_tests, project
    ):
        assert project.errored is True
        assert project.queried is True
        assert any(test.status == "error" for test in explore_tests)

    def test_running_tests_should_be_empty(self, validator_after_run):
        assert len(validator_after_run._test_by_task_id) == 0

    def test_ignored_dimensions_should_not_be_queried(
        self, validator_after_run, project
    ):
        explore = project.models[0].explores[0]
        assert not any(dim.queried for dim in explore.dimensions if dim.ignore is True)

    def test_should_be_one_explore_test_per_explore(self, explore_tests, project):
        assert project.count_explores() == len(explore_tests)

    def test_should_be_one_dimension_test_per_dimension(self, dimension_tests, project):
        explore = project.get_explore("eye_exam", "users__fail")
        assert len(explore.dimensions) == len(dimension_tests)

    def test_all_tests_should_be_unique(self, explore_tests, dimension_tests):
        assert len(set(explore_tests)) == len(explore_tests)
        assert len(set(dimension_tests)) == len(dimension_tests)


def test_create_and_run_keyboard_interrupt_cancels_queries(validator):
    validator._test_by_task_id = {
        "abc": SqlTest(
            queries=[Query("12345")],
            lookml_ref=None,
            query_task_id="abc",
            explore_url="https://example.looker.com/x/12345",
        )
    }
    mock__run_tests = create_autospec(validator._run_tests)
    mock__run_tests.side_effect = KeyboardInterrupt()
    validator._run_tests = mock__run_tests
    mock_cancel_queries = create_autospec(validator._cancel_queries)
    validator._cancel_queries = mock_cancel_queries
    try:
        validator.run_tests(
            [
                SqlTest(
                    queries=[Query(67890)],
                    lookml_ref=None,
                    query_task_id="def",
                    explore_url="https://example.looker.com/x/56789",
                )
            ]
        )
    except SpectaclesException:
        mock_cancel_queries.assert_called_once_with(query_task_ids=["abc"])


def test_get_running_query_tasks(validator):
    tests = [
        SqlTest(
            queries=[Query(query_id="12345")],
            lookml_ref=None,
            query_task_id="abc",
            explore_url="https://example.looker.com/x/12345",
        ),
        SqlTest(
            queries=[Query(query_id="67890")],
            lookml_ref=None,
            query_task_id="def",
            explore_url="https://example.looker.com/x/67890",
        ),
    ]
    validator._running_tests = tests
    assert list(validator._test_by_task_id.keys()) == ["abc", "def"]


@patch("spectacles.validators.sql.LookerClient.cancel_query_task")
def test_cancel_queries(mock_client_cancel, validator):
    """
    Cancelling queries should result in the same number of client calls as
    query tasks IDs passed in, with the corresponding query task IDs called.

    """
    query_task_ids = ["A", "B", "C"]
    validator._cancel_queries(query_task_ids)
    for task_id in query_task_ids:
        mock_client_cancel.assert_any_call(task_id)


def test_extract_error_details_error_dict(validator):
    message = "An error message."
    message_details = "Shocking details."
    sql = "SELECT * FROM orders"
    query_result = {
        "status": "error",
        "data": {
            "errors": [{"message": message, "message_details": message_details}],
            "sql": sql,
        },
    }
    extracted = validator._extract_error_details(query_result)
    assert extracted["message"] == f"{message} {message_details}"
    assert extracted["sql"] == sql


def test_extract_error_details_error_list(validator):
    message = "An error message."
    query_result = {"status": "error", "data": [message]}
    extracted = validator._extract_error_details(query_result)
    assert extracted["message"] == message
    assert extracted["sql"] is None


def test_extract_error_details_error_other(validator):
    query_result = {"status": "error", "data": "some string"}
    with pytest.raises(TypeError):
        validator._extract_error_details(query_result)


def test_extract_error_details_error_non_str_message_details(validator):
    message = {"message": "An error messsage.", "details": "More details."}
    sql = "SELECT * FROM orders"
    query_result = {
        "status": "error",
        "data": {"errors": [{"message_details": message}], "sql": sql},
    }
    with pytest.raises(TypeError):
        validator._extract_error_details(query_result)


def test_extract_error_details_no_message_details(validator):
    message = "An error message."
    query_result = {
        "status": "error",
        "data": {"errors": [{"message": message, "message_details": None}]},
    }
    extracted = validator._extract_error_details(query_result)
    assert extracted["message"] == message
    assert extracted["sql"] is None


def test_extract_error_details_error_loc_wo_line(validator):
    message = "An error message."
    sql = "SELECT x FROM orders"
    query_result = {
        "status": "error",
        "data": {
            "errors": [{"message": message, "sql_error_loc": {"character": 8}}],
            "sql": sql,
        },
    }
    extracted = validator._extract_error_details(query_result)
    assert extracted["message"] == message
    assert extracted["sql"] == sql


def test_extract_error_details_error_and_v1_warning(validator):
    error_message = "An error message."
    warning_message = (
        "Note: This query contains derived tables with conditional SQL for Development Mode. "
        "Query results in Production Mode might be different."
    )
    sql = "SELECT x FROM orders"
    query_result = {
        "status": "error",
        "data": {
            "errors": [
                {"message": warning_message},
                {"message": error_message, "sql_error_loc": {"character": 8}},
            ],
            "sql": sql,
        },
    }
    extracted = validator._extract_error_details(query_result)
    assert extracted["message"] == error_message
    assert extracted["sql"] == sql


def test_extract_error_details_v1_warning(validator):
    warning_message = (
        "Note: This query contains derived tables with conditional SQL for Development Mode. "
        "Query results in Production Mode might be different."
    )
    sql = "SELECT x FROM orders"
    query_result = {
        "status": "error",
        "data": {"errors": [{"message": warning_message}], "sql": sql},
    }
    extracted = validator._extract_error_details(query_result)
    assert extracted is None


def test_extract_error_details_error_and_v2_warning(validator):
    error_message = "An error message."
    warning_message = (
        "Note: This query contains derived tables with Development Mode filters. "
        "Query results in Production Mode might be different."
    )
    sql = "SELECT x FROM orders"
    query_result = {
        "status": "error",
        "data": {
            "errors": [
                {"message": warning_message},
                {"message": error_message, "sql_error_loc": {"character": 8}},
            ],
            "sql": sql,
        },
    }
    extracted = validator._extract_error_details(query_result)
    assert extracted["message"] == error_message
    assert extracted["sql"] == sql


def test_extract_error_details_v2_warning(validator):
    warning_message = (
        "Note: This query contains derived tables with Development Mode filters. "
        "Query results in Production Mode might be different."
    )
    sql = "SELECT x FROM orders"
    query_result = {
        "status": "error",
        "data": {"errors": [{"message": warning_message}], "sql": sql},
    }
    extracted = validator._extract_error_details(query_result)
    assert extracted is None
