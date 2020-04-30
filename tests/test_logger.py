from pathlib import Path
from spectacles.logger import log_sql_error
from spectacles.exceptions import SqlError


def test_logging_failing_explore_sql(tmpdir):
    error = SqlError(
        path="example_explore",
        message="example error message",
        sql="select example_explore.example_dimension_1 from model",
        explore_url="https://example.looker.com/x/12345",
    )

    query_directory = Path(tmpdir / "queries")
    query_directory.mkdir(exist_ok=True)
    query_file = Path(query_directory / "explore_model__example_explore.sql")

    log_sql_error(error, tmpdir, "explore_model", "example_explore")
    content = open(query_file).read()

    assert Path.exists(query_file)
    assert content == "select example_explore.example_dimension_1 from model"


def test_logging_failing_dimension_sql(tmpdir):
    error = SqlError(
        path="example_explore",
        message="example error message",
        sql="select example_explore.example_dimension_1 from model",
        explore_url="https://example.looker.com/x/12345",
    )

    query_directory = Path(tmpdir / "queries")
    query_directory.mkdir(exist_ok=True)
    query_file = (
        query_directory
        / "explore_model__example_explore__example_explore.example_dimension_1.sql"
    )

    log_sql_error(
        error,
        tmpdir,
        "explore_model",
        "example_explore",
        "example_explore.example_dimension_1",
    )

    content = open(query_file).read()

    assert content == "select example_explore.example_dimension_1 from model"
    assert Path.exists(query_file)
