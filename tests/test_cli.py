import os
import sys
from collections import defaultdict
from unittest.mock import patch, Mock
from unittest import TestCase
import yaml
import pytest
import click
from click.testing import CliRunner
from tests.constants import TEST_BASE_URL, ENV_VARS
from fonz.cli import create_parser, main, connect, sql, handle_exceptions
from fonz.exceptions import FonzException, ValidationError
import logging


@pytest.fixture
def clean_env(monkeypatch):
    for variable in ENV_VARS.keys():
        monkeypatch.delenv(variable, raising=False)


@pytest.fixture
def env(monkeypatch):
    for variable, value in ENV_VARS.items():
        monkeypatch.setenv(variable, value)


@pytest.fixture
def limited_env(monkeypatch):
    for variable, value in ENV_VARS.items():
        if variable in ["LOOKER_CLIENT_SECRET", "LOOKER_PROJECT"]:
            monkeypatch.delenv(variable, raising=False)
        else:
            monkeypatch.setenv(variable, value)


@pytest.fixture()
def parser():
    parser = create_parser()
    return parser


@patch("sys.argv", new=["fonz", "--help"])
def test_help(parser,):
    with pytest.raises(SystemExit) as cm:
        main()
        assert cm.value.code == 0


@pytest.mark.parametrize(
    "exception,exit_code",
    [(ValueError, 1), (FonzException, 100), (ValidationError, 102)],
)
def test_handle_exceptions_unhandled_error(exception, exit_code):
    @handle_exceptions
    def raise_exception():
        raise exception(f"This is a {exception.__class__.__name__}.")

    with pytest.raises(SystemExit) as pytest_error:
        raise_exception()

    assert pytest_error.value.code == exit_code


@patch(
    "sys.argv",
    new=[
        "fonz",
        "connect",
        "--base-url",
        "cli_url",
        "--client-id",
        "cli_client_id",
        "--client-secret",
        "cli_client_secret",
    ],
)
@patch("fonz.cli.connect")
def test_connect_with_base_cli(mock_connect, clean_env):
    main()
    mock_connect.assert_called_once_with(
        "cli_url", "cli_client_id", "cli_client_secret", 19999, 3.1
    )


@patch(
    "sys.argv",
    new=[
        "fonz",
        "connect",
        "--base-url",
        "cli_url",
        "--client-id",
        "cli_client_id",
        "--client-secret",
        "cli_client_secret",
        "--port",
        "272727",
        "--api-version",
        "3.1",
    ],
)
@patch("builtins.open")
@patch("fonz.cli.connect")
def test_connect_with_full_cli(mock_connect, mock_open, clean_env):
    main()
    mock_connect.assert_called_once_with(
        "cli_url", "cli_client_id", "cli_client_secret", 272727, 3.1
    )


@patch("sys.argv", new=["fonz", "connect"])
@patch("fonz.cli.connect")
def test_connect_with_env_variables(mock_connect, env):
    main()
    mock_connect.assert_called_once_with(
        "https://test.looker.com",
        "CLIENT_ID_ENV_VAR",
        "CLIENT_SECRET_ENV_VAR",
        19999,
        3.1,
    )


@patch("sys.argv", new=["fonz", "connect", "--config-file", "config.yml"])
@patch("fonz.cli.YamlConfigAction.parse_config")
@patch("builtins.open")
@patch("fonz.cli.connect")
def test_connect_with_config_file(
    mock_connect, mock_open, mock_parse_config, clean_env
):
    mock_parse_config.return_value = {
        "base_url": TEST_BASE_URL,
        "client_id": "CLIENT_ID_CONFIG",
        "client_secret": "CLIENT_SECRET_CONFIG",
    }
    main()
    mock_connect.assert_called_once_with(
        "https://test.looker.com",
        "CLIENT_ID_CONFIG",
        "CLIENT_SECRET_CONFIG",
        19999,
        3.1,
    )


@patch("sys.argv", new=["fonz", "connect"])
def test_connect_no_arguments(clean_env):
    with pytest.raises(SystemExit) as cm:
        main()
        assert cm.value.code == 1


@patch(
    "sys.argv",
    new=[
        "fonz",
        "connect",
        "--client-id",
        "cli_client_id",
        "--client-secret",
        "cli_client_secret",
    ],
)
@patch("fonz.cli.connect")
def test_connect_with_limited_env_variables(mock_connect, env):
    main()
    mock_connect.assert_called_once_with(
        "https://test.looker.com", "cli_client_id", "cli_client_secret", 19999, 3.1
    )


@patch(
    "sys.argv",
    new=[
        "fonz",
        "sql",
        "--base-url",
        "cli_url",
        "--client-id",
        "cli_client_id",
        "--client-secret",
        "cli_client_secret",
        "--project",
        "cli_project",
        "--branch",
        "cli_branch",
    ],
)
@patch("fonz.cli.sql")
def test_sql_with_base_cli_without_batch(mock_sql, clean_env):
    main()
    mock_sql.assert_called_once_with(
        "cli_project",
        "cli_branch",
        ["*.*"],
        "cli_url",
        "cli_client_id",
        "cli_client_secret",
        19999,
        3.1,
        False,
    )


@patch(
    "sys.argv",
    new=[
        "fonz",
        "sql",
        "--base-url",
        "cli_url",
        "--client-id",
        "cli_client_id",
        "--client-secret",
        "cli_client_secret",
        "--project",
        "cli_project",
        "--branch",
        "cli_branch",
        "--batch",
    ],
)
@patch("fonz.cli.sql")
def test_sql_with_base_cli_with_batch(mock_sql, clean_env):
    main()
    mock_sql.assert_called_once_with(
        "cli_project",
        "cli_branch",
        ["*.*"],
        "cli_url",
        "cli_client_id",
        "cli_client_secret",
        19999,
        3.1,
        True,
    )


@patch(
    "sys.argv",
    new=[
        "fonz",
        "sql",
        "--base-url",
        "cli_url",
        "--client-id",
        "cli_client_id",
        "--client-secret",
        "cli_client_secret",
        "--port",
        "272727",
        "--api-version",
        "3.1",
        "--project",
        "cli_project",
        "--branch",
        "cli_branch",
    ],
)
@patch("fonz.cli.sql")
def test_sql_with_full_cli(mock_sql, clean_env):
    main()
    mock_sql.assert_called_once_with(
        "cli_project",
        "cli_branch",
        ["*.*"],
        "cli_url",
        "cli_client_id",
        "cli_client_secret",
        272727,
        3.1,
        False,
    )


@patch("sys.argv", new=["fonz", "sql", "--batch"])
@patch("fonz.cli.sql", autospec=True)
def test_sql_with_env_variables(mock_sql, env):
    main()
    mock_sql.assert_called_once_with(
        "PROJECT_ENV_VAR",
        "BRANCH_ENV_VAR",
        ["*.*"],
        "https://test.looker.com",
        "CLIENT_ID_ENV_VAR",
        "CLIENT_SECRET_ENV_VAR",
        19999,
        3.1,
        True,
    )


@patch("sys.argv", new=["fonz", "sql", "--config-file", "config.yml"])
@patch("fonz.cli.YamlConfigAction.parse_config")
@patch("builtins.open")
@patch("fonz.cli.sql")
def test_sql_with_config_file(mock_sql, mock_open, mock_parse_config, clean_env):
    mock_parse_config.return_value = {
        "base_url": TEST_BASE_URL,
        "client_id": "CLIENT_ID_CONFIG",
        "client_secret": "CLIENT_SECRET_CONFIG",
        "project": "PROJECT_ENV_VAR",
        "branch": "BRANCH_ENV_VAR",
    }

    main()
    mock_sql.assert_called_once_with(
        "PROJECT_ENV_VAR",
        "BRANCH_ENV_VAR",
        ["*.*"],
        "https://test.looker.com",
        "CLIENT_ID_CONFIG",
        "CLIENT_SECRET_CONFIG",
        19999,
        3.1,
        False,
    )


@patch("sys.argv", new=["fonz", "sql"])
def test_sql_no_arguments(clean_env):
    with pytest.raises(SystemExit) as cm:
        main()
        assert cm.value.code == 1


@patch(
    "sys.argv",
    new=[
        "fonz",
        "sql",
        "--client-id",
        "cli_client_id",
        "--client-secret",
        "cli_client_secret",
    ],
)
@patch("fonz.cli.sql", autospec=True)
def test_sql_with_limited_env_variables(mock_connect, env):
    main()
    mock_connect.assert_called_once_with(
        "PROJECT_ENV_VAR",
        "BRANCH_ENV_VAR",
        ["*.*"],
        "https://test.looker.com",
        "cli_client_id",
        "cli_client_secret",
        19999,
        3.1,
        False,
    )
