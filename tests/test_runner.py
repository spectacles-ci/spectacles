import pytest
from spectacles.exceptions import ApiConnectionError


def test_validate_sql_with_import_projects_error(runner):
    runner.import_projects = True
    with pytest.raises(ApiConnectionError):
        runner.validate_sql(["*/*"], [])


def test_validate_assert_with_import_projects_error(runner):
    runner.import_projects = True
    with pytest.raises(ApiConnectionError):
        runner.validate_sql()
