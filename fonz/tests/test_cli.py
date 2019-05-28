import os
from unittest.mock import patch, Mock
import pytest
from click.testing import CliRunner
from fonz.tests.constants import TEST_BASE_URL
from fonz.cli import connect
import logging


@pytest.fixture(scope="class")
def runner(request):
    """Click's CLI runner to invoke commands as command line scripts."""
    request.cls.runner = CliRunner()


@pytest.mark.usefixtures("runner")
class TestConnect(object):
    def test_help(self):
        result = self.runner.invoke(connect, ["--help"])
        assert result.exit_code == 0

    def test_no_arguments_exits_with_nonzero_code(self):
        result = self.runner.invoke(connect)
        assert result.exit_code != 0

    @patch("fonz.cli.Fonz", autospec=True)
    def test_with_command_line_args_only(self, mock_client):
        result = self.runner.invoke(
            connect,
            [
                TEST_BASE_URL,
                "--client-id",
                "FAKE_CLIENT_ID",
                "--client-secret",
                "FAKE_CLIENT_SECRET",
            ],
        )
        mock_client.assert_called_once_with(
            TEST_BASE_URL, "FAKE_CLIENT_ID", "FAKE_CLIENT_SECRET", 19999, "3.0"
        )
        mock_client.return_value.connect.assert_called_once()
        assert result.exit_code == 0

    @patch("fonz.cli.Fonz", autospec=True)
    @patch.dict(
        os.environ,
        {
            "LOOKER_BASE_URL": TEST_BASE_URL,
            "LOOKER_CLIENT_ID": "FAKE_CLIENT_ID",
            "LOOKER_CLIENT_SECRET": "FAKE_CLIENT_SECRET",
        },
    )
    def test_with_env_vars_only(self, mock_client):
        result = self.runner.invoke(connect)
        mock_client.assert_called_once_with(
            TEST_BASE_URL, "FAKE_CLIENT_ID", "FAKE_CLIENT_SECRET", 19999, "3.0"
        )
        mock_client.return_value.connect.assert_called_once()
        assert result.exit_code == 0

    def test_with_config_file_only(self):
        pass

    def test_with_config_file_args_and_env_vars(self):
        pass
