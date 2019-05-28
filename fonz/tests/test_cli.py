import os
from unittest.mock import patch
import pytest
from click.testing import CliRunner
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

    @patch("fonz.connection.Fonz.connect")
    def test_with_command_line_args_only(self, mock_connect):
        result = self.runner.invoke(
            connect,
            [
                "https://test.looker.com",
                "--client-id",
                "FAKE_CLIENT_ID",
                "--client-secret",
                "FAKE_CLIENT_SECRET",
            ],
        )
        assert result.exit_code == 0

    @patch("fonz.connection.Fonz.connect")
    @patch.dict(
        os.environ,
        {
            "LOOKER_BASE_URL": "https://test.looker.com",
            "LOOKER_CLIENT_ID": "FAKE_CLIENT_ID",
            "LOOKER_CLIENT_SECRET": "FAKE_CLIENT_SECRET",
        },
    )
    def test_with_env_vars_only(self, mock_connect):
        result = self.runner.invoke(connect)
        assert result.exit_code == 0

    def test_with_config_file_only(self):
        pass

    def test_with_config_file_args_and_env_vars(self):
        pass
