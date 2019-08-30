import os
import sys
from unittest.mock import patch, Mock
from unittest import TestCase
import yaml
import pytest
import click
from click.testing import CliRunner
from tests.constants import TEST_BASE_URL, ENV_VARS
from fonz.cli import create_parser, main, connect, sql
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
        "cli_url", "cli_client_id", "cli_client_secret", "19999", "3.0"
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
@patch("fonz.cli.connect")
def test_connect_with_full_cli(mock_connect, clean_env):
    main()
    mock_connect.assert_called_once_with(
        "cli_url", "cli_client_id", "cli_client_secret", "272727", "3.1"
    )


@patch("sys.argv", new=["fonz", "connect"])
@patch("fonz.cli.connect")
def test_connect_with_env_variables(mock_connect, env):
    main()
    mock_connect.assert_called_once_with(
        "https://test.looker.com",
        "CLIENT_ID_ENV_VAR",
        "CLIENT_SECRET_ENV_VAR",
        "19999",
        "3.0",
    )


@patch("sys.argv", new=["fonz", "connect", "--config-file", "config.yml"])
@patch("fonz.cli.yaml.load")
@patch("fonz.cli.connect")
def test_connect_with_config_file(mock_connect, mock_yaml_load, clean_env):
    mock_yaml_load.return_value = {
        "base_url": TEST_BASE_URL,
        "client_id": "CLIENT_ID_CONFIG",
        "client_secret": "CLIENT_SECRET_CONFIG",
    }
    main()
    mock_connect.assert_called_once_with(
        "https://test.looker.com",
        "CLIENT_ID_CONFIG",
        "CLIENT_SECRET_CONFIG",
        "19999",
        "3.0",
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
        "https://test.looker.com", "cli_client_id", "cli_client_secret", "19999", "3.0"
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
        "cli_url",
        "cli_client_id",
        "cli_client_secret",
        "19999",
        "3.0",
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
        "cli_url",
        "cli_client_id",
        "cli_client_secret",
        "19999",
        "3.0",
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
        "cli_url",
        "cli_client_id",
        "cli_client_secret",
        "272727",
        "3.1",
        False,
    )


@patch("sys.argv", new=["fonz", "sql", "--batch"])
@patch("fonz.cli.sql")
def test_sql_with_env_variables(mock_sql, env):
    main()
    mock_sql.assert_called_once_with(
        "PROJECT_ENV_VAR",
        "BRANCH_ENV_VAR",
        "https://test.looker.com",
        "CLIENT_ID_ENV_VAR",
        "CLIENT_SECRET_ENV_VAR",
        "19999",
        "3.0",
        True,
    )


@patch("sys.argv", new=["fonz", "sql", "--config-file", "config.yml"])
@patch("fonz.cli.yaml.load")
@patch("fonz.cli.sql")
def test_sql_with_config_file(mock_sql, mock_yaml_load, clean_env):
    mock_yaml_load.return_value = {
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
        "https://test.looker.com",
        "CLIENT_ID_CONFIG",
        "CLIENT_SECRET_CONFIG",
        "19999",
        "3.0",
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
@patch("fonz.cli.sql")
def test_sql_with_limited_env_variables(mock_connect, env):
    main()
    mock_connect.assert_called_once_with(
        "PROJECT_ENV_VAR",
        "BRANCH_ENV_VAR",
        "https://test.looker.com",
        "cli_client_id",
        "cli_client_secret",
        "19999",
        "3.0",
        False,
    )


@patch("fonz.cli.Fonz")
def test_connect(mock_fonz, clean_env):
    connect("https://test.looker.com", "client_id", "client_secret", "19999", "3.0")
    mock_fonz.assert_called_once_with(
        "https://test.looker.com", "client_id", "client_secret", "19999", "3.0"
    )


@patch("fonz.cli.Fonz")
def test_sql(mock_fonz, clean_env):
    sql(
        "project",
        "branch",
        "https://test.looker.com",
        "client_id",
        "client_secret",
        "19999",
        "3.0",
        True,
    )
    mock_fonz.assert_called_once_with(
        "https://test.looker.com",
        "client_id",
        "client_secret",
        "19999",
        "3.0",
        "project",
        "branch",
    )
