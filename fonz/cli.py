import sys
import yaml
import argparse
import os
from fonz.connection import Fonz
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
    args = parse_args(parser)

    if args.command == "connect":
        connect(
            args.base_url,
            args.client_id,
            args.client_secret,
            args.config_file,
            args.port,
            args.api_version,
        )
    elif args.command == "sql":
        sql(
            args.project,
            args.branch,
            args.base_url,
            args.client_id,
            args.client_secret,
            args.config_file,
            args.port,
            args.api_version,
            args.batch,
        )


def create_parser():
    parser = argparse.ArgumentParser(prog="fonz")

    subs = parser.add_subparsers(title="Available sub-commands", dest="command")
    base_subparser = _build_base_subparser()

    connect_sub = _build_connect_subparser(subs, base_subparser)
    sql_sub = _build_sql_subparser(subs, base_subparser)

    return parser


def _build_base_subparser():
    base_subparser = argparse.ArgumentParser(add_help=False)

    base_subparser.add_argument("--base-url", default=os.environ.get("LOOKER_BASE_URL"))
    base_subparser.add_argument(
        "--client-id", default=os.environ.get("LOOKER_CLIENT_ID")
    )
    base_subparser.add_argument(
        "--client-secret", default=os.environ.get("LOOKER_CLIENT_SECRET")
    )
    base_subparser.add_argument("--port", default=19999)
    base_subparser.add_argument("--api-version", default=3.0)
    base_subparser.add_argument("--config-file", type=argparse.FileType(mode="r"))

    return base_subparser


def _build_connect_subparser(subparsers, base_subparser):
    connect_sub = subparsers.add_parser(
        "connect",
        parents=[base_subparser],
        help="Connect to Looker instance to test credentials.",
    )
    return connect_sub


def _build_sql_subparser(subparsers, base_subparser):
    sql_sub = subparsers.add_parser(
        "sql",
        parents=[base_subparser],
        help="Build and run queries to test your Looker instance.",
    )

    sql_sub.add_argument("--project", default=os.environ.get("LOOKER_PROJECT"))
    sql_sub.add_argument("--branch", default=os.environ.get("LOOKER_GIT_BRANCH"))
    sql_sub.add_argument("--batch", action="store_true")

    return sql_sub


def parse_args(parser):
    args = parser.parse_args()

    if args.config_file:
        data = yaml.load(args.config_file)
        arg_dict = args.__dict__
        for key, value in data.items():
            if isinstance(value, list):
                arg_dict[key].extend(value)
            else:
                arg_dict[key] = value
    return args


def connect(base_url, client_id, client_secret, config_file, port, api_version):
    client = Fonz(base_url, client_id, client_secret, port, api_version)
    client.connect()


def sql(
    project,
    branch,
    base_url,
    client_id,
    client_secret,
    config_file,
    port,
    api_version,
    batch,
):
    client = Fonz(
        base_url, client_id, client_secret, port, api_version, project, branch
    )
    client.connect()
    client.update_session()
    client.build_project()
    client.validate(batch)
    client.report_results(batch)


if __name__ == "__main__":
    main()
