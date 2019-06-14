import os
from unittest.mock import patch, Mock
import yaml
import pytest
import click
from click.testing import CliRunner
from tests.constants import TEST_BASE_URL, ENV_VARS
from fonz.cli import connect, sql
import logging


@pytest.fixture(scope="class")
def runner(request):
    """Click's CLI runner to invoke commands as command line scripts."""
    request.cls.runner = CliRunner()


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


@pytest.mark.usefixtures("runner")
class TestConnect(object):
    def test_help(self):
        result = self.runner.invoke(
            connect, ["--help"], standalone_mode=False, catch_exceptions=False
        )
        assert result.exit_code == 0

    def test_no_arguments_exits_with_nonzero_code(self, clean_env):
        result = self.runner.invoke(connect)
        assert result.exit_code != 0

    @patch("fonz.cli.Fonz", autospec=True)
    def test_with_command_line_args_only(self, mock_client, clean_env):
        result = self.runner.invoke(
            connect,
            [
                "--base-url",
                TEST_BASE_URL,
                "--client-id",
                "CLIENT_ID_CLI",
                "--client-secret",
                "CLIENT_SECRET_CLI",
            ],
            standalone_mode=False,
            catch_exceptions=False,
        )
        mock_client.assert_called_once_with(
            TEST_BASE_URL, "CLIENT_ID_CLI", "CLIENT_SECRET_CLI", 19999, "3.0"
        )
        mock_client.return_value.connect.assert_called_once()
        assert result.exit_code == 0

    @patch("fonz.cli.Fonz", autospec=True)
    def test_with_env_vars_only(self, mock_client, env):
        result = self.runner.invoke(
            connect, standalone_mode=False, catch_exceptions=False
        )
        mock_client.assert_called_once_with(
            TEST_BASE_URL, "CLIENT_ID_ENV_VAR", "CLIENT_SECRET_ENV_VAR", 19999, "3.0"
        )
        mock_client.return_value.connect.assert_called_once()
        assert result.exit_code == 0

    @patch("fonz.cli.Fonz", autospec=True)
    def test_with_config_file_only(self, mock_client, clean_env):
        with self.runner.isolated_filesystem():
            with open("config.yml", "w") as file:
                config = {
                    "base_url": TEST_BASE_URL,
                    "client_id": "CLIENT_ID_CONFIG",
                    "client_secret": "CLIENT_SECRET_CONFIG",
                }
                yaml.dump(config, file)
            result = self.runner.invoke(
                connect,
                ["--config-file", "config.yml"],
                standalone_mode=False,
                catch_exceptions=False,
            )
        mock_client.assert_called_once_with(
            TEST_BASE_URL, "CLIENT_ID_CONFIG", "CLIENT_SECRET_CONFIG", 19999, "3.0"
        )
        mock_client.return_value.connect.assert_called_once()
        assert result.exit_code == 0

    @patch("fonz.cli.Fonz", autospec=True)
    def test_cli_supersedes_env_var_which_supersedes_config_file(
        self, mock_client, limited_env
    ):
        with self.runner.isolated_filesystem():
            with open("config.yml", "w") as file:
                config = {
                    "base_url": "URL_CONFIG",
                    "client_id": "CLIENT_ID_CONFIG",
                    "client_secret": "CLIENT_SECRET_CONFIG",
                }
                yaml.dump(config, file)
            result = self.runner.invoke(
                connect,
                ["--base-url", "URL_CLI", "--config-file", "config.yml"],
                standalone_mode=False,
                catch_exceptions=False,
            )
        mock_client.assert_called_once_with(
            "URL_CLI", "CLIENT_ID_ENV_VAR", "CLIENT_SECRET_CONFIG", 19999, "3.0"
        )
        mock_client.return_value.connect.assert_called_once()
        assert result.exit_code == 0


@pytest.mark.usefixtures("runner")
class TestSql(object):
    def test_help(self):
        result = self.runner.invoke(
            sql, ["--help"], standalone_mode=False, catch_exceptions=False
        )
        assert result.exit_code == 0

    def test_no_arguments_exits_with_nonzero_code(self, clean_env):
        result = self.runner.invoke(sql)
        assert result.exit_code != 0

    @patch("fonz.cli.Fonz", autospec=True)
    def test_with_command_line_args_only(self, mock_client):
        mock_client.return_value.messages = []
        result = self.runner.invoke(
            sql,
            [
                "--base-url",
                TEST_BASE_URL,
                "--client-id",
                "CLIENT_ID_CLI",
                "--client-secret",
                "CLIENT_SECRET_CLI",
                "--project",
                "PROJECT_CLI",
                "--branch",
                "BRANCH_CLI",
            ],
            standalone_mode=False,
            catch_exceptions=False,
        )
        mock_client.assert_called_once_with(
            TEST_BASE_URL,
            "CLIENT_ID_CLI",
            "CLIENT_SECRET_CLI",
            19999,
            "3.0",
            "PROJECT_CLI",
            "BRANCH_CLI",
        )
        mock_client.return_value.connect.assert_called_once()
        assert result.exit_code == 0

    @patch("fonz.cli.Fonz", autospec=True)
    def test_with_env_vars_only(self, mock_client, env):
        mock_client.return_value.messages = []
        result = self.runner.invoke(sql, standalone_mode=False, catch_exceptions=False)
        mock_client.assert_called_once_with(
            TEST_BASE_URL,
            "CLIENT_ID_ENV_VAR",
            "CLIENT_SECRET_ENV_VAR",
            19999,
            "3.0",
            "PROJECT_ENV_VAR",
            "BRANCH_ENV_VAR",
        )
        mock_client.return_value.connect.assert_called_once()
        assert result.exit_code == 0

    @patch("fonz.cli.Fonz", autospec=True)
    def test_with_config_file_only(self, mock_client, clean_env):
        mock_client.return_value.messages = []
        with self.runner.isolated_filesystem():
            with open("config.yml", "w") as file:
                config = {
                    "base_url": TEST_BASE_URL,
                    "client_id": "CLIENT_ID_CONFIG",
                    "client_secret": "CLIENT_SECRET_CONFIG",
                    "project": "PROJECT_CONFIG",
                    "branch": "BRANCH_CONFIG",
                }
                yaml.dump(config, file)
            result = self.runner.invoke(
                sql,
                ["--config-file", "config.yml"],
                standalone_mode=False,
                catch_exceptions=False,
            )
        mock_client.assert_called_once_with(
            TEST_BASE_URL,
            "CLIENT_ID_CONFIG",
            "CLIENT_SECRET_CONFIG",
            19999,
            "3.0",
            "PROJECT_CONFIG",
            "BRANCH_CONFIG",
        )
        mock_client.return_value.connect.assert_called_once()
        assert result.exit_code == 0

    @patch("fonz.cli.Fonz", autospec=True)
    def test_cli_supersedes_env_var_which_supersedes_config_file(
        self, mock_client, limited_env
    ):
        mock_client.return_value.messages = []
        with self.runner.isolated_filesystem():
            with open("config.yml", "w") as file:
                config = {
                    "base_url": "URL_CONFIG",
                    "client_id": "CLIENT_ID_CONFIG",
                    "client_secret": "CLIENT_SECRET_CONFIG",
                    "branch": "BRANCH_CONFIG",
                    "project": "PROJECT_CONFIG",
                }
                yaml.dump(config, file)
            result = self.runner.invoke(
                sql,
                [
                    "--base-url",
                    "URL_CLI",
                    "--branch",
                    "BRANCH_CLI",
                    "--config-file",
                    "config.yml",
                ],
                standalone_mode=False,
                catch_exceptions=False,
            )
        mock_client.assert_called_once_with(
            "URL_CLI",
            "CLIENT_ID_ENV_VAR",
            "CLIENT_SECRET_CONFIG",
            19999,
            "3.0",
            "PROJECT_CONFIG",
            "BRANCH_CLI",
        )
        mock_client.return_value.connect.assert_called_once()
        assert result.exit_code == 0
