import sys
import yaml
import argparse
import os
from fonz.runner import Runner
from fonz.client import LookerClient
from fonz.exceptions import FonzException, ValidationError
from fonz.logger import GLOBAL_LOGGER as logger, LOG_FILEPATH


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

    subparsers = parser.add_subparsers(title="Available sub-commands", dest="command")
    base_subparser, defaults = _build_base_subparser()

    _build_connect_subparser(subparsers, base_subparser)
    _build_sql_subparser(subparsers, base_subparser, defaults)

    return parser


def _build_base_subparser():
    base_subparser = argparse.ArgumentParser(add_help=False)
    base_subparser.add_argument("--config-file")
    args, remaining = base_subparser.parse_known_args()
    if args.config_file:
        with open(args.config_file, "r") as file:
            config_from_file = yaml.safe_load(file)
    else:
        config_from_file = {}

    env_var_arg_map = {
        "base_url": "LOOKER_BASE_URL",
        "client_id": "LOOKER_CLIENT_ID",
        "client_secret": "LOOKER_CLIENT_SECRET",
        "project": "LOOKER_PROJECT",
        "branch": "LOOKER_GIT_BRANCH",
    }

    defaults = {}
    for arg, env_var in env_var_arg_map.items():
        defaults[arg] = os.environ.get(env_var) or config_from_file.get(arg)

    base_subparser.add_argument("--base-url", default=defaults["base_url"])
    base_subparser.add_argument("--client-id", default=defaults["client_id"])
    base_subparser.add_argument("--client-secret", default=defaults["client_secret"])
    base_subparser.add_argument("--port", type=int, default=19999)
    base_subparser.add_argument("--api-version", type=float, default=3.1)

    return base_subparser, defaults


def _build_connect_subparser(subparsers, base_subparser):
    subparser = subparsers.add_parser(
        "connect",
        parents=[base_subparser],
        help="Connect to Looker instance to test credentials.",
    )


def _build_sql_subparser(subparsers, base_subparser, defaults):
    subparser = subparsers.add_parser(
        "sql",
        parents=[base_subparser],
        help="Build and run queries to test your Looker instance.",
    )

    subparser.add_argument("--project", default=defaults["project"])
    subparser.add_argument("--branch", default=defaults["branch"])
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
