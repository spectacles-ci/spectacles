from pathlib import Path
import sys
import yaml
from yaml.parser import ParserError
import argparse
import os
from typing import Callable
from fonz.runner import Runner
from fonz.client import LookerClient
from fonz.exceptions import FonzException, ValidationError
from fonz.logger import GLOBAL_LOGGER as logger, LOG_FILEPATH


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
                    action.required = False
            if not hasattr(namespace, dest) or not getattr(namespace, dest):
                setattr(namespace, dest, value)
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
        except FonzException as error:
            logger.error(
                f"{error}\n\n"
                "For support, please create an issue at "
                "https://github.com/dbanalyticsco/Fonz/issues\n"
            )
            sys.exit(error.exit_code)
        except Exception as error:
            logger.debug(error, exc_info=True)
            logger.error(
                f'Encountered unexpected {error.__class__.__name__}: "{error}"\n'
                f"Full error traceback logged to {LOG_FILEPATH}\n\n"
                "For support, please create an issue at "
                "https://github.com/dbanalyticsco/Fonz/issues\n"
            )
            sys.exit(1)

    return wrapper


@handle_exceptions
def main():
    """Runs main function. This is the entry point."""
    parser = create_parser()
    args = parser.parse_args()

    if args.command == "connect":
        connect(
            args.base_url,
            args.client_id,
            args.client_secret,
            args.port,
            args.api_version,
        )
    elif args.command == "sql":
        sql(
            args.project,
            args.branch,
            args.explores,
            args.base_url,
            args.client_id,
            args.client_secret,
            args.port,
            args.api_version,
            args.batch,
        )


def create_parser() -> argparse.ArgumentParser:
    """Creates the top-level argument parser.

    Returns:
        argparse.ArgumentParser: Top-level argument parser.

    """
    parser = argparse.ArgumentParser(prog="fonz")
    subparser_action = parser.add_subparsers(
        title="Available sub-commands", dest="command"
    )
    base_subparser = _build_base_subparser()
    _build_connect_subparser(subparser_action, base_subparser)
    _build_sql_subparser(subparser_action, base_subparser)
    return parser


def _build_base_subparser() -> argparse.ArgumentParser:
    """Returns the base subparser with arguments required for every subparser.

    Returns:
        argparse.ArgumentParser: Base subparser with url and auth arguments.

    """
    base_subparser = argparse.ArgumentParser(add_help=False)
    base_subparser.add_argument("--config-file", action=YamlConfigAction)
    base_subparser.add_argument(
        "--base-url",
        default=os.environ.get("LOOKER_BASE_URL"),
        required=False if os.environ.get("LOOKER_BASE_URL") else True,
    )
    base_subparser.add_argument(
        "--client-id",
        default=os.environ.get("LOOKER_CLIENT_ID"),
        required=False if os.environ.get("LOOKER_CLIENT_ID") else True,
    )
    base_subparser.add_argument(
        "--client-secret",
        default=os.environ.get("LOOKER_CLIENT_SECRET"),
        required=False if os.environ.get("LOOKER_CLIENT_SECRET") else True,
    )
    base_subparser.add_argument("--port", type=int, default=19999)
    base_subparser.add_argument("--api-version", type=float, default=3.1)

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
        default=os.environ.get("LOOKER_PROJECT"),
        required=False if os.environ.get("LOOKER_PROJECT") else True,
    )
    subparser.add_argument(
        "--branch",
        default=os.environ.get("LOOKER_GIT_BRANCH"),
        required=False if os.environ.get("LOOKER_GIT_BRANCH") else True,
    )
    subparser.add_argument("--explores", nargs="+", default=["*.*"])
    subparser.add_argument("--batch", action="store_true")


def connect(
    base_url: str, client_id: str, client_secret: str, port: int, api_version: float
) -> None:
    """Tests the connection and credentials for the Looker API.

    Args:
        base_url: Base URL for the Looker instance, e.g. https://mycompany.looker.com.
        client_id: Looker API client ID.
        client_secret: Looker API client secret.
        port: Desired API port to use for requests.
        api_version: Desired API version to use for requests.

    """
    LookerClient(base_url, client_id, client_secret, port, api_version)


def sql(
    project,
    branch,
    explores,
    base_url,
    client_id,
    client_secret,
    port,
    api_version,
    batch,
) -> None:
    """Runs and validates the SQL for each selected LookML dimension.

    Args:
        project: Name of the Looker project to use.
        branch: Name of the Git branch to check out.
        explores: List of selector strings in 'model_name.explore_name' format.
            The '*' wildcard selects all models or explores. For instance,
            'model_name.*' would select all explores in the 'model_name' model.
        base_url: Base URL for the Looker instance, e.g. https://mycompany.looker.com.
        client_id: Looker API client ID.
        client_secret: Looker API client secret.
        port: Desired API port to use for requests.
        api_version: Desired API version to use for requests.
        batch: When true, runs one query per explore (using all dimensions). When
            false, runs one query per dimension. Batch mode increases query speed
            but can only return the first error encountered for each dimension.

    """
    runner = Runner(
        base_url, project, branch, client_id, client_secret, port, api_version
    )
    errors = runner.validate_sql(explores, batch)
    if errors:
        raise ValidationError


if __name__ == "__main__":
    main()
