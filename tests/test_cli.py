from unittest.mock import patch
import pytest
from constants import ENV_VARS
from spectacles.cli import main, create_parser, handle_exceptions
from spectacles.exceptions import SpectaclesException, ValidationError


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


@patch("sys.argv", new=["spectacles", "--help"])
def test_help(parser,):
    with pytest.raises(SystemExit) as cm:
        main()
        assert cm.value.code == 0


@pytest.mark.parametrize(
    "exception,exit_code",
    [(ValueError, 1), (SpectaclesException, 100), (ValidationError, 102)],
)
def test_handle_exceptions_unhandled_error(exception, exit_code):
    @handle_exceptions
    def raise_exception():
        raise exception(f"This is a {exception.__class__.__name__}.")

    with pytest.raises(SystemExit) as pytest_error:
        raise_exception()

    assert pytest_error.value.code == exit_code


def test_parse_args_with_no_arguments_supplied(clean_env, parser, capsys):
    with pytest.raises(SystemExit):
        parser.parse_args(["connect"])
    captured = capsys.readouterr()
    assert (
        "the following arguments are required: --base-url, --client-id, --client-secret"
        in captured.err
    )


def test_parse_args_with_one_argument_supplied(clean_env, parser, capsys):
    with pytest.raises(SystemExit):
        parser.parse_args(["connect", "--base-url", "BASE_URL_CLI"])
    captured = capsys.readouterr()
    assert (
        "the following arguments are required: --client-id, --client-secret"
        in captured.err
    )


def test_parse_args_with_only_cli(clean_env, parser):
    args = parser.parse_args(
        [
            "connect",
            "--base-url",
            "BASE_URL_CLI",
            "--client-id",
            "CLIENT_ID_CLI",
            "--client-secret",
            "CLIENT_SECRET_CLI",
        ]
    )
    assert args.base_url == "BASE_URL_CLI"
    assert args.client_id == "CLIENT_ID_CLI"
    assert args.client_secret == "CLIENT_SECRET_CLI"


@patch("spectacles.cli.YamlConfigAction.parse_config")
def test_parse_args_with_only_config_file(mock_parse_config, parser, clean_env):
    mock_parse_config.return_value = {
        "base_url": "BASE_URL_CONFIG",
        "client_id": "CLIENT_ID_CONFIG",
        "client_secret": "CLIENT_SECRET_CONFIG",
    }
    args = parser.parse_args(["connect", "--config-file", "config.yml"])
    assert args.base_url == "BASE_URL_CONFIG"
    assert args.client_id == "CLIENT_ID_CONFIG"
    assert args.client_secret == "CLIENT_SECRET_CONFIG"


@patch("spectacles.cli.YamlConfigAction.parse_config")
def test_parse_args_with_incomplete_config_file(
    mock_parse_config, parser, clean_env, capsys
):
    mock_parse_config.return_value = {
        "base_url": "BASE_URL_CONFIG",
        "client_id": "CLIENT_ID_CONFIG",
    }
    with pytest.raises(SystemExit):
        parser.parse_args(["connect", "--config-file", "config.yml"])
    captured = capsys.readouterr()
    assert "the following arguments are required: --client-secret" in captured.err


def test_parse_args_with_only_env_vars(env, parser):
    args = parser.parse_args(["connect"])
    assert args.base_url == "BASE_URL_ENV_VAR"
    assert args.client_id == "CLIENT_ID_ENV_VAR"
    assert args.client_secret == "CLIENT_SECRET_ENV_VAR"


def test_parse_args_with_incomplete_env_vars(limited_env, parser, capsys):
    with pytest.raises(SystemExit):
        parser.parse_args(["connect"])
    captured = capsys.readouterr()
    assert "the following arguments are required: --client-secret" in captured.err


@patch("spectacles.cli.YamlConfigAction.parse_config")
def test_arg_precedence(mock_parse_config, limited_env, parser):
    # Precedence: command line > environment variables > config files
    mock_parse_config.return_value = {
        "base_url": "BASE_URL_CONFIG",
        "client_id": "CLIENT_ID_CONFIG",
        "client_secret": "CLIENT_SECRET_CONFIG",
    }
    args = parser.parse_args(
        ["connect", "--config-file", "config.yml", "--base-url", "BASE_URL_CLI"]
    )
    assert args.base_url == "BASE_URL_CLI"
    assert args.client_id == "CLIENT_ID_ENV_VAR"
    assert args.client_secret == "CLIENT_SECRET_CONFIG"


def test_env_var_override_argparse_default(env, parser):
    args = parser.parse_args(["connect"])
    assert args.port == 8080


@patch("spectacles.cli.YamlConfigAction.parse_config")
def test_config_override_argparse_default(mock_parse_config, clean_env, parser):
    mock_parse_config.return_value = {
        "base_url": "BASE_URL_CONFIG",
        "client_id": "CLIENT_ID_CONFIG",
        "client_secret": "CLIENT_SECRET_CONFIG",
        "port": 8080,
    }
    args = parser.parse_args(["connect", "--config-file", "config.yml"])
    assert args.port == 8080


@patch("spectacles.cli.YamlConfigAction.parse_config")
def test_bad_config_file_parameter(mock_parse_config, clean_env, parser):
    mock_parse_config.return_value = {
        "base_url": "BASE_URL_CONFIG",
        "api_key": "CLIENT_ID_CONFIG",
        "port": 8080,
    }
    with pytest.raises(
        SpectaclesException, match="not a valid configuration parameter"
    ):
        parser.parse_args(["connect", "--config-file", "config.yml"])


def test_parse_remote_reset_with_assert(env, parser):
    args = parser.parse_args(["assert", "--remote-reset"])
    assert args.remote_reset
