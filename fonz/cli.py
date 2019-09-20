from pathlib import Path
import sys
import yaml
import argparse
import os
from fonz.runner import Runner
from fonz.client import LookerClient
from fonz.exceptions import FonzException, ValidationError
from fonz.logger import GLOBAL_LOGGER as logger, LOG_FILEPATH


class ConfigAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        config = self.parse_config(path=values)
        for dest, value in config.items():
            for action in parser._actions:
                if dest == action.dest:
                    action.required = False
            if not hasattr(namespace, dest) or not getattr(namespace, dest):
                setattr(namespace, dest, value)
        parser.set_defaults(**config)

    def parse_config(self, path):
        raise NotImplementedError()


class YamlConfigAction(ConfigAction):
    def parse_config(self, path):
        try:
            with Path(path).open("r") as file:
                return yaml.safe_load(file)
        except (FileNotFoundError, yaml.parser.ParserError) as error:
            raise argparse.ArgumentError(self, error)


def handle_exceptions(function):
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


def create_parser():
    parser = argparse.ArgumentParser(prog="fonz")
    subparser_action = parser.add_subparsers(
        title="Available sub-commands", dest="command"
    )
    base_subparser = _build_base_subparser()
    _build_connect_subparser(subparser_action, base_subparser),
    _build_sql_subparser(subparser_action, base_subparser)
    return parser


def _build_base_subparser():
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


def _build_connect_subparser(subparser_action, base_subparser):
    subparser = subparser_action.add_parser(
        "connect",
        parents=[base_subparser],
        help="Connect to Looker instance to test credentials.",
    )


def _build_sql_subparser(subparser_action, base_subparser):
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


def connect(base_url, client_id, client_secret, port, api_version):
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
):
    runner = Runner(
        base_url, project, branch, client_id, client_secret, port, api_version
    )
    errors = runner.validate_sql(explores, batch)
    if errors:
        raise ValidationError


if __name__ == "__main__":
    main()
