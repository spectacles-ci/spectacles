from pathlib import Path
import sys
import yaml
from yaml.parser import ParserError
import argparse
import logging
import os
from typing import Callable
from spectacles import __version__
from spectacles.runner import Runner
from spectacles.client import LookerClient
from spectacles.exceptions import SpectaclesException, ValidationError
from spectacles.logger import GLOBAL_LOGGER as logger, FileFormatter
import spectacles.printer as printer

LOG_FILENAME = "spectacles.log"
LOG_FILEPATH = Path()


class ConfigFileAction(argparse.Action):
    """Parses an arbitrary config file and assigns its values as arg defaults."""

    def __call__(self, parser, namespace, values, option_string):
        """Populates argument defaults with values from the config file.

        Args:
            parser: Parent argparse parser that is calling the action.
            namespace: Object where parsed values will be set.
            values: Parsed values to be set to the namespace.
            option_string: Argument string, e.g. "--optional".

        """
        config = self.parse_config(path=values)
        for dest, value in config.items():
            for action in parser._actions:
                if dest == action.dest:
                    """Required actions that are fulfilled by config are no longer
                    required from the command line."""
                    action.required = False
                    # Override default if not previously set by an environment variable.
                    if not isinstance(action, EnvVarAction) or not os.environ.get(
                        action.env_var
                    ):
                        setattr(namespace, dest, value)
                    break
            else:
                raise SpectaclesException(
                    f"'{dest}' in {values} is not a valid configuration parameter."
                )
        parser.set_defaults(**config)

    def parse_config(self, path) -> dict:
        """Base method for parsing an arbitrary config format."""
        raise NotImplementedError()


class YamlConfigAction(ConfigFileAction):
    """Parses a YAML config file and assigns its values as argument defaults."""

    def parse_config(self, path: str) -> dict:
        """Loads a YAML config file, returning its dictionary format.

        Args:
            path: Path to the config file to be loaded.

        Returns:
            dict: Dictionary representation of the config file.

        """
        try:
            with Path(path).open("r") as file:
                return yaml.safe_load(file)
        except (FileNotFoundError, ParserError) as error:
            raise argparse.ArgumentError(self, error)


class EnvVarAction(argparse.Action):
    """Uses an argument default defined in an environment variable.

    Args:
        env_var: The name of the environment variable to get the default from.
        required: The argument's requirement status as defined in add_argument.
        default: The argument default as defined in add_argument.
        **kwargs: Arbitrary keyword arguments.

    """

    def __init__(self, env_var, required=False, default=None, **kwargs):
        self.env_var = env_var
        self.in_env = False
        if env_var in os.environ:
            default = os.environ[env_var]
            self.in_env = True
        if required and default:
            required = False
        super().__init__(default=default, required=required, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        """Sets the argument value to the namespace during parsing.

        Args:
            parser: Parent argparse parser that is calling the action.
            namespace: Object where parsed values will be set.
            values: Parsed values to be set to the namespace.
            option_string: Argument string, e.g. "--optional".

        """
        setattr(namespace, self.dest, values)


def handle_exceptions(function: Callable) -> Callable:
    """Wrapper for handling custom exceptions by logging them.

    Args:
        function: Callable to wrap and handle exceptions for.

    Returns:
        callable: Wrapped callable.

    """

    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except ValidationError as error:
            sys.exit(error.exit_code)
        except SpectaclesException as error:
            logger.error(
                f"{error}\n\n"
                + printer.dim(
                    "For support, please create an issue at "
                    "https://github.com/spectacles-ci/spectacles/issues"
                )
                + "\n"
            )
            sys.exit(error.exit_code)
        except KeyboardInterrupt as error:
            logger.debug(error, exc_info=True)
            logger.info("Spectacles was manually interrupted.")
            sys.exit(1)
        except Exception as error:
            logger.debug(error, exc_info=True)
            logger.error(
                f'Encountered unexpected {error.__class__.__name__}: "{error}"\n'
                f"Full error traceback logged to {LOG_FILEPATH}\n\n"
                + printer.dim(
                    "For support, please create an issue at "
                    "https://github.com/spectacles-ci/spectacles/issues"
                )
                + "\n"
            )
            sys.exit(1)

    return wrapper


def set_file_handler(directory: str) -> None:

    global LOG_FILEPATH

    log_directory = Path(directory)
    LOG_FILEPATH = Path(log_directory / LOG_FILENAME)
    log_directory.mkdir(exist_ok=True)

    fh = logging.FileHandler(LOG_FILEPATH)
    fh.setLevel(logging.DEBUG)

    formatter = FileFormatter("%(asctime)s %(levelname)s | %(message)s")
    fh.setFormatter(formatter)

    logger.addHandler(fh)


@handle_exceptions
def main():
    """Runs main function. This is the entry point."""
    parser = create_parser()
    args = parser.parse_args()
    for handler in logger.handlers:
        handler.setLevel(args.log_level)

    set_file_handler(args.log_dir)

    if args.command == "connect":
        run_connect(
            args.base_url,
            args.client_id,
            args.client_secret,
            args.port,
            args.api_version,
        )
    elif args.command == "sql":
        run_sql(
            args.project,
            args.branch,
            args.explores,
            args.base_url,
            args.client_id,
            args.client_secret,
            args.port,
            args.api_version,
            args.mode,
            args.remote_reset,
            args.concurrency,
        )
    elif args.command == "assert":
        run_assert(
            args.project,
            args.branch,
            args.base_url,
            args.client_id,
            args.client_secret,
            args.port,
            args.api_version,
            args.remote_reset,
        )


def create_parser() -> argparse.ArgumentParser:
    """Creates the top-level argument parser.

    Returns:
        argparse.ArgumentParser: Top-level argument parser.

    """
    parser = argparse.ArgumentParser(prog="spectacles")
    parser.add_argument("--version", action="version", version=__version__)
    subparser_action = parser.add_subparsers(
        title="Available sub-commands", dest="command"
    )
    base_subparser = _build_base_subparser()
    _build_connect_subparser(subparser_action, base_subparser)
    _build_sql_subparser(subparser_action, base_subparser)
    _build_assert_subparser(subparser_action, base_subparser)
    return parser


def _build_base_subparser() -> argparse.ArgumentParser:
    """Returns the base subparser with arguments required for every subparser.

    Returns:
        argparse.ArgumentParser: Base subparser with url and auth arguments.

    """
    base_subparser = argparse.ArgumentParser(add_help=False)
    base_subparser.add_argument(
        "--config-file",
        action=YamlConfigAction,
        help="The path to an optional YAML config file.",
    )
    base_subparser.add_argument(
        "--base-url",
        action=EnvVarAction,
        env_var="LOOKER_BASE_URL",
        required=True,
        help="The URL of your Looker instance, e.g. https://company-name.looker.com",
    )
    base_subparser.add_argument(
        "--client-id",
        action=EnvVarAction,
        env_var="LOOKER_CLIENT_ID",
        required=True,
        help="The client ID of the Looker user that spectacles will authenticate as.",
    )
    base_subparser.add_argument(
        "--client-secret",
        action=EnvVarAction,
        env_var="LOOKER_CLIENT_SECRET",
        required=True,
        help="The client secret of the Looker user that spectacles \
            will authenticate as.",
    )
    base_subparser.add_argument(
        "--port",
        type=int,
        action=EnvVarAction,
        env_var="LOOKER_PORT",
        default=19999,
        help="The port of your Looker instance’s API. The default is port 19999.",
    )
    base_subparser.add_argument(
        "--api-version",
        type=float,
        action=EnvVarAction,
        env_var="LOOKER_API_VERSION",
        default=3.1,
        help="The version of the Looker API to use. The default is version 3.1.",
    )
    base_subparser.add_argument(
        "-v",
        "--verbose",
        action="store_const",
        dest="log_level",
        const=logging.DEBUG,
        default=logging.INFO,
        help="Display debug logging during spectacles execution. \
            Useful for debugging and making bug reports.",
    )
    base_subparser.add_argument(
        "--log-dir",
        action=EnvVarAction,
        env_var="SPECTACLES_LOG_DIR",
        default="logs",
        help="The directory that Spectacles will write logs to.",
    )

    return base_subparser


def _build_connect_subparser(
    subparser_action: argparse._SubParsersAction,
    base_subparser: argparse.ArgumentParser,
) -> None:
    """Returns the subparser for the subcommand `connect`.

    Args:
        subparser_action (type): Description of parameter `subparser_action`.
        base_subparser (type): Description of parameter `base_subparser`.

    Returns:
        type: Description of returned object.

    """
    subparser_action.add_parser(
        "connect",
        parents=[base_subparser],
        help="Connect to Looker instance to test credentials.",
    )


def _build_sql_subparser(
    subparser_action: argparse._SubParsersAction,
    base_subparser: argparse.ArgumentParser,
) -> None:
    """Returns the subparser for the subcommand `sql`.

    Args:
        subparser_action: Description of parameter `subparser_action`.
        base_subparser: Description of parameter `base_subparser`.

    Returns:
        type: Description of returned object.

    """
    subparser = subparser_action.add_parser(
        "sql",
        parents=[base_subparser],
        help="Build and run queries to test your Looker instance.",
    )

    subparser.add_argument(
        "--project",
        action=EnvVarAction,
        env_var="LOOKER_PROJECT",
        required=True,
        help="The LookML project you want to test.",
    )
    subparser.add_argument(
        "--branch",
        action=EnvVarAction,
        env_var="LOOKER_GIT_BRANCH",
        required=True,
        help="The branch of your project that spectacles will use to run queries.",
    )
    subparser.add_argument(
        "--explores",
        nargs="+",
        default=["*/*"],
        help="Specify the explores spectacles should test. \
            List of selector strings in 'model_name/explore_name' format. \
            The '*' wildcard selects all models or explores. For instance,\
            'model_name/*' would select all explores in the 'model_name' model.",
    )
    subparser.add_argument(
        "--mode",
        choices=["batch", "single", "hybrid"],
        default="batch",
        help="Specify the mode the SQL validator should run.\
            In single-dimension mode, the SQL validator will run one query \
            per dimension. In batch mode, the SQL validator will create one \
            query per explore. In hybrid mode, the SQL validator will run in \
            batch mode and then run errored explores in single-dimension mode.",
    )
    subparser.add_argument(
        "--remote-reset",
        action="store_true",
        help="When set to true, the SQL validator will tell Looker to reset the \
            user's branch to the revision of the branch that is on the remote. \
            WARNING: This will delete any uncommited changes in the user's workspace.",
    )
    subparser.add_argument(
        "--concurrency",
        default=10,
        type=int,
        help="Specify how many concurrent queries you want to have running \
            against your data warehouse. The default is 10.",
    )


def _build_assert_subparser(
    subparser_action: argparse._SubParsersAction,
    base_subparser: argparse.ArgumentParser,
) -> None:
    """Returns the subparser for the subcommand `assert`.

    Args:
        subparser_action: Description of parameter `subparser_action`.
        base_subparser: Description of parameter `base_subparser`.

    Returns:
        type: Description of returned object.

    """
    subparser = subparser_action.add_parser(
        "assert", parents=[base_subparser], help="Run Looker data tests."
    )

    subparser.add_argument(
        "--project", action=EnvVarAction, env_var="LOOKER_PROJECT", required=True
    )
    subparser.add_argument(
        "--branch", action=EnvVarAction, env_var="LOOKER_GIT_BRANCH", required=True
    )
    subparser.add_argument(
        "--remote-reset",
        action="store_true",
        help="When set to true, the SQL validator will tell Looker to reset the \
            user's branch to the revision of the branch that is on the remote. \
            WARNING: This will delete any uncommited changes in the user's workspace.",
    )


def run_connect(
    base_url: str, client_id: str, client_secret: str, port: int, api_version: float
) -> None:
    """Tests the connection and credentials for the Looker API."""
    LookerClient(base_url, client_id, client_secret, port, api_version)


def run_assert(
    project, branch, base_url, client_id, client_secret, port, api_version, remote_reset
) -> None:
    runner = Runner(
        base_url,
        project,
        branch,
        client_id,
        client_secret,
        port,
        api_version,
        remote_reset,
    )
    errors = runner.validate_data_tests()
    if errors:
        for error in sorted(errors, key=lambda x: x["path"]):
            printer.print_data_test_error(error)
        logger.info("")
        raise ValidationError
    else:
        logger.info("")


def run_sql(
    project,
    branch,
    explores,
    base_url,
    client_id,
    client_secret,
    port,
    api_version,
    mode,
    remote_reset,
    concurrency,
) -> None:
    """Runs and validates the SQL for each selected LookML dimension."""
    runner = Runner(
        base_url,
        project,
        branch,
        client_id,
        client_secret,
        port,
        api_version,
        remote_reset,
    )
    errors = runner.validate_sql(explores, mode, concurrency)
    if errors:
        for error in sorted(errors, key=lambda x: x["path"]):
            printer.print_sql_error(error)
        logger.info("")
        raise ValidationError
    else:
        logger.info("")


if __name__ == "__main__":
    main()
