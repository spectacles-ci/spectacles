import pytest
from spectacles.exceptions import LookerApiError


def test_validate_sql_with_import_projects_error(runner):
    runner.import_projects = True
    with pytest.raises(LookerApiError):
        runner.validate_sql(["*/*"], [])


def test_validate_assert_with_import_projects_error(runner):
    runner.import_projects = True
    with pytest.raises(LookerApiError):
        runner.validate_sql()
