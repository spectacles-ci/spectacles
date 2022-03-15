from pathlib import Path
import sys
import re
import platform
import yaml
import json
from yaml.parser import ParserError
import argparse
import logging
import os
from typing import Callable
from spectacles import __version__
from spectacles.runner import Runner
from spectacles.client import LookerClient
from spectacles.exceptions import (
    LookerApiError,
    SpectaclesException,
    GenericValidationError,
)
from spectacles.logger import GLOBAL_LOGGER as logger, set_file_handler
import spectacles.printer as printer
import spectacles.tracking as tracking
from spectacles.utils import log_duration


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
                    name="invalid-config-file-param",
                    title="Invalid configuration file parameter.",
                    detail=f"Parameter '{dest}' in {values} is not valid.",
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
            raise argparse.ArgumentError(self, str(error))


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
        if env_var in os.environ:
            default = os.environ[env_var]
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


class EnvVarStoreTrueAction(argparse._StoreTrueAction):
    def __init__(self, env_var, required=False, default=False, **kwargs):
        self.env_var = env_var
        if env_var in os.environ:
            value = os.environ[env_var].lower()
            if value not in ("true", "false"):
                raise SpectaclesException(
                    name="invalid-env-var-value",
                    title="Invalid value for environment variable.",
                    detail=(
                        f"Allowed values for {env_var} are 'true' or 'false' "
                        f"(case-insensitive), received '{value}'"
                    ),
                )
            default = True if value == "true" else False
        if required and default:
            required = False
        super().__init__(default=default, required=required, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, True)


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
        except GenericValidationError as error:
            sys.exit(error.exit_code)
        except LookerApiError as error:
            logger.error(
                f"\n{error}\n\n"
                + printer.dim(
                    "Run in verbose mode (-v) or check your log file to see the full "
                    "response from the Looker API. "
                    "For support, please create an issue at "
                    "https://github.com/spectacles-ci/spectacles/issues"
                )
                + "\n"
            )
            looker_api_response = json.dumps(error.looker_api_response, indent=2)
            logger.debug(
                f"Spectacles received a {error.status} response code from "
                f"the Looker API with the following details: {looker_api_response}\n"
            )
            sys.exit(error.exit_code)
        except SpectaclesException as error:
            logger.error(
                f"\n{error}\n\n"
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
                f'\nEncountered unexpected {error.__class__.__name__}: "{error}"\n'
                f"Full error traceback logged to file.\n\n"
                + printer.dim(
                    "For support, please create an issue at "
                    "https://github.com/spectacles-ci/spectacles/issues"
                )
                + "\n"
            )
            sys.exit(1)

    return wrapper


def preprocess_dash(arg: str) -> str:
    """Replace any dashes with tildes, otherwise argparse will assume they're options"""
    return re.sub(r"^-(?=([\w_\*]+/[\w_\*]+)|(\d+)$)", "~", arg)


def restore_dash(arg: str) -> str:
    """Convert leading tildes back to dashes."""
    return re.sub(r"^~", "-", arg)


@handle_exceptions
def main():
    """Runs main function. This is the entry point."""
    if sys.version_info < (3, 7):
        raise SpectaclesException(
            name="insufficient-python-version",
            title="Spectacles requires Python 3.7 or higher.",
            detail="The current Python version is %s." % platform.python_version(),
        )

    # Convert leading `-` to `~` so they don't break `parse_args`
    args = [preprocess_dash(arg) for arg in sys.argv[1:]]
    parser = create_parser()
    args = parser.parse_args(args)

    branch = getattr(args, "branch", None)
    commit_ref = getattr(args, "commit_ref", None)
    ref = branch or commit_ref
    target = getattr(args, "target", None)
    incremental = getattr(args, "incremental", None)

    # Normally would be cleaner to handle this with an argparse mutually exclusive
    # group, but this doesn't work with --commit-ref and --remote-reset also needing
    # to be mutually exclusive, so raise the error manually.
    if branch and commit_ref:
        parser.error("argument --commit-ref not allowed with argument --branch")

    if target and not incremental:
        parser.error(
            "argument --target can only be passed in incremental mode (--incremental)"
        )

    for handler in logger.handlers:
        handler.setLevel(args.log_level)

    set_file_handler(args.log_dir)

    if not args.do_not_track:
        invocation_id = tracking.track_invocation_start(
            args.base_url,
            args.command,
            project=args.project if args.command != "connect" else None,
        )

    if args.command == "connect":
        run_connect(
            base_url=args.base_url,
            client_id=args.client_id,
            client_secret=args.client_secret,
            port=args.port,
            api_version=args.api_version,
        )
    elif args.command == "sql":
        run_sql(
            log_dir=args.log_dir,
            project=args.project,
            ref=ref,
            filters=[restore_dash(arg) for arg in args.explores],
            base_url=args.base_url,
            client_id=args.client_id,
            client_secret=args.client_secret,
            port=args.port,
            api_version=args.api_version,
            fail_fast=args.fail_fast,
            incremental=args.incremental,
            target=args.target,
            remote_reset=args.remote_reset,
            concurrency=args.concurrency,
            profile=args.profile,
            runtime_threshold=args.runtime_threshold,
            chunk_size=args.chunk_size,
        )
    elif args.command == "assert":
        run_assert(
            project=args.project,
            ref=ref,
            filters=[restore_dash(arg) for arg in args.explores],
            base_url=args.base_url,
            client_id=args.client_id,
            client_secret=args.client_secret,
            port=args.port,
            api_version=args.api_version,
            remote_reset=args.remote_reset,
        )
    elif args.command == "content":
        run_content(
            project=args.project,
            ref=ref,
            filters=[restore_dash(arg) for arg in args.explores],
            base_url=args.base_url,
            client_id=args.client_id,
            client_secret=args.client_secret,
            port=args.port,
            api_version=args.api_version,
            remote_reset=args.remote_reset,
            incremental=args.incremental,
            target=args.target,
            exclude_personal=args.exclude_personal,
            folders=[restore_dash(arg) for arg in args.folders],
        )
    elif args.command == "lookml":
        run_lookml(
            project=args.project,
            ref=ref,
            base_url=args.base_url,
            client_id=args.client_id,
            client_secret=args.client_secret,
            port=args.port,
            api_version=args.api_version,
            remote_reset=args.remote_reset,
            severity=args.severity,
        )

    if not args.do_not_track:
        tracking.track_invocation_end(
            args.base_url,
            args.command,
            invocation_id,
            args.project if args.command != "connect" else None,
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
    _build_lookml_subparser(subparser_action, base_subparser)
    _build_sql_subparser(subparser_action, base_subparser)
    _build_assert_subparser(subparser_action, base_subparser)
    _build_content_subparser(subparser_action, base_subparser)
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
        help="The client ID of the Looker user that Spectacles will authenticate as.",
    )
    base_subparser.add_argument(
        "--client-secret",
        action=EnvVarAction,
        env_var="LOOKER_CLIENT_SECRET",
        required=True,
        help="The client secret of the Looker user that Spectacles \
            will authenticate as.",
    )
    base_subparser.add_argument(
        "--port",
        type=int,
        action=EnvVarAction,
        env_var="LOOKER_PORT",
        help="The port of your Looker instance’s API. The default is port 443 (HTTPS) for GCP-hosted instances and 19999 for legacy instances.",
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
        help="Display debug logging during Spectacles execution. \
            Useful for debugging and making bug reports.",
    )
    base_subparser.add_argument(
        "--log-dir",
        action=EnvVarAction,
        env_var="SPECTACLES_LOG_DIR",
        default="logs",
        help="The directory that Spectacles will write logs to.",
    )
    base_subparser.add_argument(
        "--do-not-track",
        action=EnvVarStoreTrueAction,
        env_var="SPECTACLES_DO_NOT_TRACK",
        help="Disables anonymised event tracking.",
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


def _build_validator_subparser(
    subparser_action: argparse._SubParsersAction,
    base_subparser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    """Returns the base subparser with arguments required for every validator.

    Returns:
        argparse.ArgumentParser: validator subparser with project, branch, remote reset and import projects arguments.

    """
    base_subparser.add_argument(
        "--project",
        action=EnvVarAction,
        env_var="LOOKER_PROJECT",
        required=True,
        help="The LookML project you want to test.",
    )
    base_subparser.add_argument(
        "--branch",
        action=EnvVarAction,
        env_var="LOOKER_GIT_BRANCH",
        help="The branch of your project that Spectacles will use to run queries.",
    )
    group = base_subparser.add_mutually_exclusive_group()
    group.add_argument(
        "--remote-reset",
        action=EnvVarStoreTrueAction,
        env_var="SPECTACLES_REMOTE_RESET",
        help="When set to true, the SQL validator will tell Looker to reset the \
            user's branch to the revision of the branch that is on the remote. \
            WARNING: This will delete any uncommited changes in the user's workspace.",
    )
    group.add_argument(
        "--commit-ref",
        action=EnvVarAction,
        env_var="LOOKER_COMMIT_REF",
        help="The commit of your project that Spectacles will test against. \
            In order to test a specific commit, Spectacles will create a new branch \
            for the tests and then delete the branch when it is finished.",
    )
    return base_subparser


def _build_select_subparser(
    subparser_action: argparse._SubParsersAction,
    base_subparser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    base_subparser.add_argument(
        "--explores",
        nargs="+",
        default=["*/*"],
        help="Specify the explores Spectacles should test. \
            List of strings in 'model_name/explore_name' format. \
            The '*' wildcard selects all models or explores. For instance,\
            'model_name/*' would select all explores in the 'model_name' model.",
    )
    return base_subparser


def _build_lookml_subparser(
    subparser_action: argparse._SubParsersAction,
    base_subparser: argparse.ArgumentParser,
) -> None:
    """Returns the subparser for the subcommand `lookml`.

    Args:
        subparser_action (type): Description of parameter `subparser_action`.
        base_subparser (type): Description of parameter `base_subparser`.

    Returns:
        type: Description of returned object.

    """
    subparser = subparser_action.add_parser(
        "lookml",
        parents=[base_subparser],
        help="Test for LookML syntax errors.",
    )
    subparser.add_argument(
        "--severity",
        choices=["success", "info", "warning", "error", "fatal"],
        default="warning",
        help=(
            "Specify a level of validation error severity to trigger test failure. "
            "Spectacles will display all errors, regardless of severity, "
            "but only errors at or higher than this level will cause this "
            "validator to fail. The default is 'warning'."
        ),
    )
    _build_validator_subparser(subparser_action, subparser)


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
        help="Run SQL queries to test your Looker instance.",
    )
    group = subparser.add_mutually_exclusive_group()
    group.add_argument(
        "--fail-fast",
        action="store_true",
        help=(
            "Test explore-by-explore instead of dimension-by-dimension. "
            "This means that validation takes less time but only returns the first "
            "error identified in each explore. "
        ),
    )
    group.add_argument(
        "--incremental",
        action="store_true",
        help=(
            "Only display errors which are not present on the target branch or commit. "
            "If --target is not specified, Spectacles compares to production."
        ),
    )
    subparser.add_argument(
        "--target",
        help=(
            "The branch name or commit SHA to compare to for incremental testing. "
            "Must be used with --incremental."
        ),
    )
    subparser.add_argument(
        "--concurrency",
        default=10,
        type=int,
        help="Specify how many concurrent queries you want to have running \
            against your data warehouse. The default is 10.",
    )
    subparser.add_argument(
        "-p",
        "--profile",
        action="store_true",
        help=(
            "After validation, display queries that took longer than the runtime "
            "threshold (5 seconds by default) to complete."
        ),
    )
    subparser.add_argument(
        "--runtime-threshold",
        type=int,
        default=5,
        help=(
            "When profiling, only display queries that ran longer than this value in "
            "seconds."
        ),
    )
    subparser.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        help="Limit the size of explore-level queries by this number of dimensions.",
    )
    _build_validator_subparser(subparser_action, subparser)
    _build_select_subparser(subparser_action, subparser)


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
    _build_validator_subparser(subparser_action, subparser)
    _build_select_subparser(subparser_action, subparser)


def _build_content_subparser(
    subparser_action: argparse._SubParsersAction,
    base_subparser: argparse.ArgumentParser,
) -> None:
    subparser = subparser_action.add_parser(
        "content", parents=[base_subparser], help="Run Looker content validation."
    )
    subparser.add_argument(
        "--incremental",
        action="store_true",
        help=(
            "Only display errors which are not present on the target branch or commit. "
            "If --target is not specified, Spectacles compares to production."
        ),
    )
    subparser.add_argument(
        "--target",
        help=(
            "The branch name or commit SHA to compare to for incremental testing. "
            "Must be used with --incremental."
        ),
    )
    subparser.add_argument(
        "--exclude-personal",
        action="store_true",
        help="Exclude errors found in content in personal folders.",
    )
    subparser.add_argument(
        "--folders",
        nargs="+",
        help=(
            "Specify the content folder IDs that Spectacles should test. "
            "Spectacles will also test all content "
            "found in subfolders of the specified folders. "
            "Appending '-' to a folder ID will exclude it and all subfolders. "
            "Takes precedence over --exclude-personal."
        ),
        default=[],
    )
    _build_validator_subparser(subparser_action, subparser)
    _build_select_subparser(subparser_action, subparser)


def run_connect(
    base_url: str, client_id: str, client_secret: str, port: int, api_version: float
) -> None:
    """Tests the connection and credentials for the Looker API."""
    LookerClient(base_url, client_id, client_secret, port, api_version)


@log_duration
def run_lookml(
    project,
    ref,
    base_url,
    client_id,
    client_secret,
    port,
    api_version,
    remote_reset,
    severity,
) -> None:
    client = LookerClient(base_url, client_id, client_secret, port, api_version)
    runner = Runner(client, project, remote_reset)
    results = runner.validate_lookml(ref, severity)
    errors = sorted(results["errors"], key=lambda x: x["metadata"]["file_path"] or "a")
    unique_files = sorted(
        set(
            error["metadata"]["file_path"]
            for error in errors
            if error["metadata"]["file_path"]
        )
    )

    for file_path in unique_files:
        printer.print_validation_result(status="failed", source=file_path)

    if errors:
        for error in errors:
            printer.print_lookml_error(
                error["metadata"]["file_path"],
                error["metadata"]["line_number"],
                error["metadata"]["severity"],
                error["message"],
                error["metadata"]["lookml_url"],
            )
        logger.info("")
        if results["status"] == "failed":
            raise GenericValidationError
    else:
        printer.print_lookml_success()
        logger.info("")


@log_duration
def run_content(
    project,
    ref,
    filters,
    base_url,
    client_id,
    client_secret,
    port,
    api_version,
    remote_reset,
    incremental,
    target,
    exclude_personal,
    folders,
) -> None:
    client = LookerClient(base_url, client_id, client_secret, port, api_version)
    runner = Runner(client, project, remote_reset)
    results = runner.validate_content(
        ref,
        filters,
        incremental,
        target,
        exclude_personal,
        folders,
    )

    for test in sorted(results["tested"], key=lambda x: (x["model"], x["explore"])):
        message = f"{test['model']}.{test['explore']}"
        printer.print_validation_result(status=test["status"], source=message)

    errors = sorted(
        results["errors"],
        key=lambda x: (x["model"], x["explore"], x["metadata"]["field_name"]),
    )
    if errors:
        for error in errors:
            printer.print_content_error(
                error["model"],
                error["explore"],
                error["message"],
                error["metadata"]["content_type"],
                error["metadata"].get("tile_type"),
                error["metadata"].get("tile_title"),
                error["metadata"]["space"],
                error["metadata"]["title"],
                error["metadata"]["url"],
            )
        logger.info("")
        raise GenericValidationError
    else:
        logger.info("")


@log_duration
def run_assert(
    project,
    ref,
    filters,
    base_url,
    client_id,
    client_secret,
    port,
    api_version,
    remote_reset,
) -> None:
    client = LookerClient(base_url, client_id, client_secret, port, api_version)
    runner = Runner(client, project, remote_reset)

    results = runner.validate_data_tests(ref, filters)

    for test in sorted(results["tested"], key=lambda x: (x["model"], x["explore"])):
        message = f"{test['model']}.{test['explore']}"
        printer.print_validation_result(status=test["status"], source=message)

    errors = sorted(
        results["errors"],
        key=lambda x: (x["model"], x["explore"], x["metadata"]["test_name"]),
    )
    if errors:
        for error in errors:
            printer.print_data_test_error(
                error["model"],
                error["explore"],
                error["metadata"]["test_name"],
                error["message"],
                error["metadata"]["lookml_url"],
            )
        logger.info("")
        raise GenericValidationError
    else:
        logger.info("")


@log_duration
def run_sql(
    log_dir,
    project,
    ref,
    filters,
    base_url,
    client_id,
    client_secret,
    port,
    api_version,
    fail_fast,
    incremental,
    target,
    remote_reset,
    concurrency,
    profile,
    runtime_threshold,
    chunk_size,
) -> None:
    """Runs and validates the SQL for each selected LookML dimension."""
    client = LookerClient(base_url, client_id, client_secret, port, api_version)
    runner = Runner(client, project, remote_reset)

    results = runner.validate_sql(
        ref,
        filters,
        fail_fast,
        incremental,
        target,
        concurrency,
        profile,
        runtime_threshold,
        chunk_size,
    )

    for test in sorted(results["tested"], key=lambda x: (x["model"], x["explore"])):
        message = f"{test['model']}.{test['explore']}"
        printer.print_validation_result(status=test["status"], source=message)

    errors = sorted(
        results["errors"],
        key=lambda x: (x["model"], x["explore"], x["metadata"].get("dimension")),
    )

    if errors:
        for error in errors:
            printer.print_sql_error(
                model=error["model"],
                explore=error["explore"],
                message=error["message"],
                sql=error["metadata"]["sql"],
                log_dir=log_dir,
                dimension=error["metadata"].get("dimension"),
                lookml_url=error["metadata"].get("lookml_url"),
            )
        if fail_fast:
            logger.info(
                printer.dim(
                    "\n\nTo determine the exact dimensions responsible for "
                    f"{'this error' if len(errors) == 1 else 'these errors'}, "
                    "you can rerun \nSpectacles without --fail-fast."
                )
            )

        logger.info("")
        raise GenericValidationError
    else:
        logger.info("")


if __name__ == "__main__":
    main()
