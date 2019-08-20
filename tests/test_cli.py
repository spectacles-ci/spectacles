import os
import sys
from unittest.mock import patch, Mock
from unittest import TestCase
import yaml
import pytest
import click
from click.testing import CliRunner
from tests.constants import TEST_BASE_URL, ENV_VARS
from fonz.cli import create_parser, main
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


def test_help(parser):
    with pytest.raises(SystemExit) as cm:
        parsed = parser.parse_args(["--help"])
        assert cm.value.code == 0


@patch("fonz.cli.connect", autospec=True)
def test_connect_with_base_cli(mock_connect, clean_env):
    with patch.object(
        sys,
        "argv",
        [
            "fonz",
            "connect",
            "--base-url",
            "cli_url",
            "--client-id",
            "cli_client_id",
            "--client-secret",
            "cli_client_secret",
        ],
    ):
        main()
        mock_connect.assert_called_once_with(
            "cli_url", "cli_client_id", "cli_client_secret", 19999, 3.0
        )


@patch("fonz.cli.connect", autospec=True)
def test_connect_with_full_cli(mock_connect, clean_env):
    with patch.object(
        sys,
        "argv",
        [
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
    ):
        main()
        mock_connect.assert_called_once_with(
            "cli_url", "cli_client_id", "cli_client_secret", "272727", "3.1"
        )


@patch("fonz.cli.connect", autospec=True)
def test_connect_with_env_variables(mock_connect, env):
    with patch.object(sys, "argv", ["fonz", "connect"]):
        main()
        mock_connect.assert_called_once_with(
            "https://test.looker.com",
            "CLIENT_ID_ENV_VAR",
            "CLIENT_SECRET_ENV_VAR",
            19999,
            3.0,
        )


@patch("fonz.cli.yaml.load")
@patch("fonz.cli.connect", autospec=True)
def test_connect_with_config_file(mock_connect, mock_yaml_load, clean_env):
    mock_yaml_load.return_value = {
        "base_url": TEST_BASE_URL,
        "client_id": "CLIENT_ID_CONFIG",
        "client_secret": "CLIENT_SECRET_CONFIG",
    }
    with patch.object(sys, "argv", ["fonz", "connect", "--config-file", "config.yml"]):
        main()
        mock_connect.assert_called_once_with(
            "https://test.looker.com",
            "CLIENT_ID_CONFIG",
            "CLIENT_SECRET_CONFIG",
            19999,
            3.0,
        )
