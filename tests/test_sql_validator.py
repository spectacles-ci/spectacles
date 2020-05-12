from typing import Iterable, Tuple, Dict
from collections import defaultdict
from unittest.mock import patch, create_autospec
import pytest
import jsonschema
import vcr
from spectacles.validators import SqlValidator, Query, QueryResult
from spectacles.exceptions import SpectaclesException

EXPECTED_QUERY_COUNTS = {"models": 1, "explores": 1, "dimensions": 5}


@pytest.fixture(scope="class")
def validator(looker_client, record_mode) -> Iterable[SqlValidator]:
    with vcr.use_cassette(
        "tests/cassettes/test_sql_validator/fixture_validator_init.yaml",
        match_on=["uri", "method", "raw_body"],
        filter_headers=["Authorization"],
        record_mode=record_mode,
    ):
        validator = SqlValidator(looker_client, project="eye_exam")
        yield validator


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
class TestBuildProject:
    def test_model_explore_dimension_counts_should_match(self, validator):
        validator.build_project(selectors=["eye_exam/users"])
        assert len(validator.project.models) == EXPECTED_QUERY_COUNTS["models"]
        assert (
            len(validator.project.models[0].explores)
            == EXPECTED_QUERY_COUNTS["explores"]
        )
        dimensions = validator.project.models[0].explores[0].dimensions
        assert len(dimensions) == EXPECTED_QUERY_COUNTS["dimensions"]
        assert "users.city" in [dim.name for dim in dimensions]
        assert not validator.project.errored
        assert validator.project.queried is False

    def test_project_with_everything_excluded_should_not_have_models(self, validator):
        validator.build_project(exclusions=["eye_exam/*"])
        assert len(validator.project.models) == 0

    def test_duplicate_selectors_should_be_deduplicated(self, validator):
        validator.build_project(selectors=["eye_exam/users", "eye_exam/users"])
        assert len(validator.project.models) == 1

    def test_invalid_model_selector_should_raise_error(self, validator):
        with pytest.raises(SpectaclesException):
            validator.build_project(selectors=["dummy/*"])


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
class TestBuildUnconfiguredProject:
    """Test for a build error when building an unconfigured LookML project."""

    def test_project_with_no_configured_models_should_raise_error(self, validator):
        validator.project.name = "eye_exam_unconfigured"
        validator.client.update_workspace(
            project="eye_exam_unconfigured", workspace="production"
        )
        with pytest.raises(SpectaclesException):
            validator.build_project()


class TestValidatePass:
    """Test the eye_exam Looker project on master for an explore without errors.

    Tests in this class often use `pytest.mark.parametrize` with the argument
    `indirect=True`. This argument allows us to parameterize the validator fixture to
    run in batch, hybrid, and/or single mode for each test. The parameters
    are passed from `parametrize` to the `mode` argument of `validator.validate`
    via a special built-in pytest fixture called `request`.

    """

    @pytest.fixture(scope="class")
    def validator_pass(
        self, request, record_mode, validator
    ) -> Iterable[Tuple[SqlValidator, Dict]]:
        mode = request.param
        with vcr.use_cassette(
            f"tests/cassettes/test_sql_validator/fixture_validator_pass[{mode}].yaml",
            match_on=["uri", "method", "raw_body"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            validator.build_project(selectors=["eye_exam/users"])
            results = validator.validate(mode)
            yield validator, results

    @pytest.mark.parametrize(
        "validator_pass", ["batch", "single", "hybrid"], indirect=True
    )
    def test_should_set_errored_and_queried(self, validator_pass):
        validator = validator_pass[0]
        assert validator.project.errored is False
        assert validator.project.queried is True

    @pytest.mark.parametrize("validator_pass", ["batch"], indirect=True)
    def test_in_batch_mode_should_run_one_query(self, validator_pass):
        validator = validator_pass[0]
        assert len(validator._query_by_task_id) == 1

    @pytest.mark.parametrize("validator_pass", ["single"], indirect=True)
    def test_in_single_mode_should_run_n_queries(self, validator_pass):
        validator = validator_pass[0]
        assert len(validator._query_by_task_id) == EXPECTED_QUERY_COUNTS["dimensions"]

    @pytest.mark.parametrize("validator_pass", ["hybrid"], indirect=True)
    def test_in_hybrid_mode_should_run_one_query(self, validator_pass):
        validator = validator_pass[0]
        assert len(validator._query_by_task_id) == 1

    @pytest.mark.parametrize(
        "validator_pass", ["batch", "single", "hybrid"], indirect=True
    )
    def test_running_queries_should_be_empty(self, validator_pass):
        validator = validator_pass[0]
        assert len(validator._running_queries) == 0

    @pytest.mark.parametrize("validator_pass", ["hybrid", "single"], indirect=True)
    def test_in_hybrid_or_single_mode_dimensions_should_be_queried(
        self, validator_pass
    ):
        validator = validator_pass[0]
        explore = validator.project.models[0].explores[0]
        assert all(dim.queried for dim in explore.dimensions if dim.ignore is False)
        assert explore.queried is True

    @pytest.mark.parametrize("validator_pass", ["batch", "single"], indirect=True)
    def test_ignored_dimensions_are_not_queried(self, validator_pass):
        validator = validator_pass[0]
        explore = validator.project.models[0].explores[0]
        assert not any(dim.queried for dim in explore.dimensions if dim.ignore is True)

    @pytest.mark.parametrize("validator_pass", ["batch"], indirect=True)
    def test_count_explores(self, validator_pass):
        validator = validator_pass[0]
        assert validator._count_explores() == 1

    @pytest.mark.parametrize("validator_pass", ["batch", "single"], indirect=True)
    def test_results_should_conform_to_schema(self, schema, validator_pass):
        results = validator_pass[1]
        jsonschema.validate(results, schema)


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
class TestValidateFail:
    @pytest.fixture(scope="class")
    def validator_fail(
        self, record_mode, validator
    ) -> Iterable[Tuple[SqlValidator, Dict]]:
        with vcr.use_cassette(
            f"tests/cassettes/test_sql_validator/fixture_validator_fail.yaml",
            match_on=["uri", "method", "raw_body"],
            filter_headers=["Authorization"],
            record_mode=record_mode,
        ):
            validator.build_project(selectors=["eye_exam/users__fail"])
            results = validator.validate(mode="hybrid")
            yield validator, results

    def test_in_hybrid_mode_should_run_n_queries(self, validator_fail):
        validator = validator_fail[0]
        assert (
            len(validator._query_by_task_id) == 1 + EXPECTED_QUERY_COUNTS["dimensions"]
        )

    def test_should_set_errored_and_queried(self, validator_fail):
        validator = validator_fail[0]
        assert validator.project.errored is True
        assert validator.project.queried is True

    def test_running_queries_should_be_empty(self, validator_fail):
        validator = validator_fail[0]
        assert len(validator._running_queries) == 0

    def test_results_should_conform_to_schema(self, schema, validator_fail):
        results = validator_fail[1]
        jsonschema.validate(results, schema)


def test_create_and_run_keyboard_interrupt_cancels_queries(validator):
    validator._running_queries = [
        Query(
            query_id="12345",
            lookml_ref=None,
            query_task_id="abc",
            explore_url="https://example.looker.com/x/12345",
        )
    ]
    mock_create_queries = create_autospec(validator._create_queries)
    mock_create_queries.side_effect = KeyboardInterrupt()
    validator._create_queries = mock_create_queries
    mock_cancel_queries = create_autospec(validator._cancel_queries)
    validator._cancel_queries = mock_cancel_queries
    try:
        validator._create_and_run(mode="batch")
    except SpectaclesException:
        mock_cancel_queries.assert_called_once_with(query_task_ids=["abc"])


def test_get_running_query_tasks(validator):
    queries = [
        Query(
            query_id="12345",
            lookml_ref=None,
            query_task_id="abc",
            explore_url="https://example.looker.com/x/12345",
        ),
        Query(
            query_id="67890",
            lookml_ref=None,
            query_task_id="def",
            explore_url="https://example.looker.com/x/67890",
        ),
    ]
    validator._running_queries = queries
    assert validator.get_running_query_tasks() == ["abc", "def"]


def test_parse_selectors_should_handle_duplicates():
    expected = defaultdict(set, model_one=set(["explore_one"]))
    assert (
        SqlValidator.parse_selectors(["model_one/explore_one", "model_one/explore_one"])
        == expected
    )


def test_parse_selectors_should_handle_same_explore_in_different_model():
    expected = defaultdict(
        set, model_one=set(["explore_one"]), model_two=set(["explore_one"])
    )
    assert (
        SqlValidator.parse_selectors(["model_one/explore_one", "model_two/explore_one"])
        == expected
    )


def test_parse_selectors_with_invalid_format_should_raise_error():
    with pytest.raises(SpectaclesException):
        SqlValidator.parse_selectors(["model_one.explore_one", "model_two:explore_one"])


@patch("spectacles.validators.LookerClient.cancel_query_task")
def test_cancel_queries(mock_client_cancel, validator):
    """
    Cancelling queries should result in the same number of client calls as
    query tasks IDs passed in, with the corresponding query task IDs called.

    """
    query_task_ids = ["A", "B", "C"]
    validator._cancel_queries(query_task_ids)
    for task_id in query_task_ids:
        mock_client_cancel.assert_any_call(task_id)


def test_handle_running_query(validator, dimension):
    query_task_id = "sakgwj392jfkajgjcks"
    query = Query(
        query_id="19428",
        lookml_ref=dimension,
        query_task_id=query_task_id,
        explore_url="https://spectacles.looker.com/x/qCJsodAZ2Y22QZLbmD0Gvy",
    )
    query_result = QueryResult(query_task_id=query_task_id, status="running")
    validator._running_queries = [query]
    validator._query_by_task_id[query_task_id] = query
    returned_sql_error = validator._handle_query_result(query_result)

    assert validator._running_queries == [query]
    assert not returned_sql_error


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
