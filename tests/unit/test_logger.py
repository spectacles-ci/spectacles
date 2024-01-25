from pathlib import Path

from spectacles.exceptions import SqlError
from spectacles.logger import log_sql_error


def test_logging_failing_explore_sql(tmpdir: Path, sql_error: SqlError) -> None:
    sql_error.metadata["dimension"] = None
    expected_directory = Path(tmpdir) / "queries"
    expected_directory.mkdir(exist_ok=True)

    log_sql_error(
        sql_error.model,
        sql_error.explore,
        sql_error.metadata["sql"],
        str(tmpdir),
        sql_error.metadata["dimension"],
    )
    expected_path = expected_directory / "eye_exam__users.sql"

    assert Path.exists(expected_path)
    with expected_path.open("r") as file:
        content = file.read()
    assert content == "SELECT age FROM users WHERE 1=2 LIMIT 1"


def test_logging_failing_dimension_sql(tmpdir: Path, sql_error: SqlError) -> None:
    expected_directory = Path(tmpdir) / "queries"
    expected_directory.mkdir(exist_ok=True)

    log_sql_error(
        sql_error.model,
        sql_error.explore,
        sql_error.metadata["sql"],
        str(tmpdir),
        sql_error.metadata["dimension"],
    )
    expected_path = expected_directory / "eye_exam__users__users_age.sql"

    assert Path.exists(expected_path)
    with expected_path.open("r") as file:
        content = file.read()
    assert content == "SELECT age FROM users WHERE 1=2 LIMIT 1"
