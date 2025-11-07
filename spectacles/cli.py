import argparse
import asyncio
import importlib.metadata
import json
import logging
import os
import platform
import re
import sys
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Union, cast

import httpx
import yaml
from yaml.parser import ParserError

import spectacles.printer as printer
from spectacles.client import (
    DEFAULT_API_VERSION,
    LOOKML_VALIDATION_TIMEOUT,
    LookerClient,
)
from spectacles.exceptions import (
    GenericValidationError,
    LookerApiError,
    SpectaclesException,
)
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.logger import set_file_handler
from spectacles.runner import Runner
from spectacles.utils import log_duration
from spectacles.validators.data_test import DATA_TEST_CONCURRENCY

__version__ = importlib.metadata.version("spectacles")


class ConfigFileAction(argparse.Action):
    """Parses an arbitrary config file and assigns its values as arg defaults."""

    def __call__(
        self,
        parser: ArgumentParser,
        namespace: Namespace,
        values: Union[str, Sequence[Any], None],
        option_string: Optional[str] = None,
    ) -> None:
        """Populates argument defaults with values from the config file.

        Args:
            parser: Parent argparse parser that is calling the action.
            namespace: Object where parsed values will be set.
            values: Parsed values to be set to the namespace.
            option_string: Argument string, e.g. "--optional".

        """
        config = self.parse_config(path=values)  # type: ignore
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

    def parse_config(self, path: str) -> Dict[str, Any]:
        """Base method for parsing an arbitrary config format."""
        raise NotImplementedError()


class YamlConfigAction(ConfigFileAction):
    """Parses a YAML config file and assigns its values as argument defaults."""

    def parse_config(self, path: str) -> Dict[str, Any]:
        """Loads a YAML config file, returning its dictionary format.

        Args:
            path: Path to the config file to be loaded.

        Returns:
            dict: Dictionary representation of the config file.

        """
        try:
            with Path(path).open("r") as file:
                return cast(Dict[str, Any], yaml.safe_load(file))
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

    def __init__(
        self,
        env_var: str,
        required: bool = False,
        default: Optional[str] = None,
        **kwargs: Any,
    ):
        self.env_var = env_var
        if env_var in os.environ:
            default = os.environ[env_var]
        if required and default:
            required = False
        super().__init__(default=default, required=required, **kwargs)

    def __call__(
        self,
        parser: ArgumentParser,
        namespace: Namespace,
        values: Union[str, Sequence[Any], None],
        option_string: Optional[str] = None,
    ) -> None:
        """Sets the argument value to the namespace during parsing.

        Args:
            parser: Parent argparse parser that is calling the action.
            namespace: Object where parsed values will be set.
            values: Parsed values to be set to the namespace.
            option_string: Argument string, e.g. "--optional".

        """
        setattr(namespace, self.dest, values)


class EnvVarStoreTrueAction(argparse._StoreTrueAction):
    def __init__(
        self, env_var: str, required: bool = False, default: bool = False, **kwargs: Any
    ):
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

    def __call__(
        self,
        parser: ArgumentParser,
        namespace: Namespace,
        values: Union[str, Sequence[Any], None],
        option_string: Optional[str] = None,
    ) -> None:
        setattr(namespace, self.dest, True)


def handle_exceptions(function: Callable[..., Any]) -> Callable[..., Any]:
    """Wrapper for handling custom exceptions by logging them.

    Args:
        function: Callable to wrap and handle exceptions for.

    Returns:
        callable: Wrapped callable.

    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:
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


def process_pin_imports(input: List[str]) -> dict[str, str]:
    return dict(arg.split(":") for arg in input)


@handle_exceptions
def main() -> None:
    """Runs main function. This is the entry point."""
    if sys.version_info < (3, 9):
        raise SpectaclesException(
            name="insufficient-python-version",
            title="Spectacles requires Python 3.9 or higher.",
            detail="The current Python version is %s." % platform.python_version(),
        )

    # Convert leading `-` to `~` so they don't break `parse_args`
    inputs = [preprocess_dash(arg) for arg in sys.argv[1:]]
    parser = create_parser()
    args = parser.parse_args(inputs)

    no_looker_ci_warning = getattr(args, "no_looker_ci_warning", False)

    branch = getattr(args, "branch", None)
    commit_ref = getattr(args, "commit_ref", None)
    ref = branch or commit_ref
    target = getattr(args, "target", None)
    incremental = getattr(args, "incremental", None)

    pin_imports = process_pin_imports(getattr(args, "pin_imports", []))

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

    if args.command == "connect":
        asyncio.run(
            run_connect(
                base_url=args.base_url,
                client_id=args.client_id,
                client_secret=args.client_secret,
                port=args.port,
                api_version=args.api_version,
            )
        )
    elif args.command == "sql":
        asyncio.run(
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
                pin_imports=pin_imports,
                ignore_hidden=args.ignore_hidden,
                use_personal_branch=args.use_personal_branch,
                result_format=(
                    "json_detail" if args.use_legacy_result_format else "json_bi"
                ),
            )
        )
    elif args.command == "assert":
        asyncio.run(
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
                pin_imports=pin_imports,
                use_personal_branch=args.use_personal_branch,
                concurrency=args.concurrency,
            )
        )
    elif args.command == "content":
        asyncio.run(
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
                pin_imports=pin_imports,
                use_personal_branch=args.use_personal_branch,
            )
        )
    elif args.command == "lookml":
        asyncio.run(
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
                pin_imports=pin_imports,
                use_personal_branch=args.use_personal_branch,
                timeout=args.timeout,
            )
        )

    # print out announcement about Looker CI Public Preview
    if not no_looker_ci_warning:
        printer.print_looker_ci_warning()


def create_parser() -> ArgumentParser:
    """Creates the top-level argument parser.

    Returns:
        ArgumentParser: Top-level argument parser.

    """
    parser = ArgumentParser(prog="spectacles")
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


def _build_base_subparser() -> ArgumentParser:
    """Returns the base subparser with arguments required for every subparser.

    Returns:
        ArgumentParser: Base subparser with url and auth arguments.

    """
    base_subparser = ArgumentParser(add_help=False)
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
        default=DEFAULT_API_VERSION,
        help="The version of the Looker API to use. The default is version 4.0.",
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
        "--no-looker-ci-warning",
        action=EnvVarStoreTrueAction,
        env_var="SPECTACLES_NO_LOOKER_CI_WARNING",
        help="Flag to suppress the announcement about Looker CI's Public Preview.",
    )
    base_subparser.add_argument(
        "--do-not-track",
        action=EnvVarStoreTrueAction,
        env_var="SPECTACLES_DO_NOT_TRACK",
        help="[DEPRECATED] Tracking is disabled by default, this argument is unused.",
    )

    return base_subparser


def _build_connect_subparser(
    subparser_action: argparse._SubParsersAction,  # type: ignore[type-arg]
    base_subparser: ArgumentParser,
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
    subparser_action: argparse._SubParsersAction,  # type: ignore[type-arg]
    base_subparser: ArgumentParser,
) -> ArgumentParser:
    """Returns the base subparser with arguments required for every validator.

    Returns:
        ArgumentParser: validator subparser with project, branch, remote reset and import projects arguments.

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
    base_subparser.add_argument(
        "--use-personal-branch",
        action=EnvVarStoreTrueAction,
        env_var="SPECTACLES_USE_PERSONAL_BRANCH",
        help="Use the user's personal branch instead of creating a temporary branch for the tests.",
    )
    base_subparser.add_argument(
        "--pin-imports",
        nargs="+",
        default=[],
        help="Pin locally imported Looker projects to a specific ref (Git branch or commit) during validation. \
            Provide these arguments in project_name:ref format.",
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
    subparser_action: argparse._SubParsersAction,  # type: ignore[type-arg]
    base_subparser: ArgumentParser,
) -> ArgumentParser:
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
    subparser_action: argparse._SubParsersAction,  # type: ignore[type-arg]
    base_subparser: ArgumentParser,
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
    subparser.add_argument(
        "--timeout",
        type=int,
        default=LOOKML_VALIDATION_TIMEOUT,
        help="Specify the timeout for the LookML validation in seconds.",
    )
    _build_validator_subparser(subparser_action, subparser)


def _build_sql_subparser(
    subparser_action: argparse._SubParsersAction,  # type: ignore[type-arg]
    base_subparser: ArgumentParser,
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
            "threshold (5 seconds by default) to complete. Must use the legacy "
            "result format (--use-legacy-result-format) if enabling profiler."
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
    subparser.add_argument(
        "--ignore-hidden",
        action="store_true",
        help=("Exclude hidden fields from validation."),
    )
    subparser.add_argument(
        "--use-legacy-result-format",
        action="store_true",
        help="Use the deprecated json_detail result format when creating queries with the Looker API (instead of the default, json_bi).",
    )
    _build_validator_subparser(subparser_action, subparser)
    _build_select_subparser(subparser_action, subparser)


def _build_assert_subparser(
    subparser_action: argparse._SubParsersAction,  # type: ignore[type-arg]
    base_subparser: ArgumentParser,
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

    subparser.add_argument(
        "--concurrency",
        type=int,
        default=DATA_TEST_CONCURRENCY,
        help=(
            "Specify the number of concurrent queries you want to have running "
            f"against your data warehouse. The default is {DATA_TEST_CONCURRENCY}."
        ),
    )


def _build_content_subparser(
    subparser_action: argparse._SubParsersAction,  # type: ignore[type-arg]
    base_subparser: ArgumentParser,
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


async def run_connect(
    base_url: str, client_id: str, client_secret: str, port: int, api_version: float
) -> None:
    """Tests the connection and credentials for the Looker API."""
    # Don't trust env to ignore .netrc credentials
    async_client = httpx.AsyncClient(trust_env=False)
    try:
        LookerClient(
            async_client, base_url, client_id, client_secret, port, api_version
        )
    finally:
        await async_client.aclose()


@log_duration
async def run_lookml(
    project: str,
    ref: str,
    base_url: str,
    client_id: str,
    client_secret: str,
    port: int,
    api_version: float,
    remote_reset: bool,
    severity: str,
    pin_imports: Dict[str, str],
    use_personal_branch: bool,
    timeout: int,
) -> None:
    # Don't trust env to ignore .netrc credentials
    async_client = httpx.AsyncClient(trust_env=False)
    try:
        client = LookerClient(
            async_client, base_url, client_id, client_secret, port, api_version
        )
        runner = Runner(client, project, remote_reset, pin_imports, use_personal_branch)

        results = await runner.validate_lookml(ref, severity, timeout)
    finally:
        await async_client.aclose()

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
async def run_content(
    project: str,
    ref: str,
    filters: List[str],
    base_url: str,
    client_id: str,
    client_secret: str,
    port: int,
    api_version: float,
    remote_reset: bool,
    incremental: bool,
    target: str,
    exclude_personal: bool,
    folders: List[str],
    pin_imports: Dict[str, str],
    use_personal_branch: bool,
) -> None:
    # Don't trust env to ignore .netrc credentials
    async_client = httpx.AsyncClient(trust_env=False)
    try:
        client = LookerClient(
            async_client, base_url, client_id, client_secret, port, api_version
        )
        runner = Runner(client, project, remote_reset, pin_imports, use_personal_branch)

        results = await runner.validate_content(
            ref,
            filters,
            incremental,
            target,
            exclude_personal,
            folders,
        )
    finally:
        await async_client.aclose()

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
                error["metadata"]["folder"],
                error["metadata"]["title"],
                error["metadata"]["url"],
            )
        logger.info("")
        raise GenericValidationError
    else:
        logger.info("")


@log_duration
async def run_assert(
    project: str,
    ref: str,
    filters: List[str],
    base_url: str,
    client_id: str,
    client_secret: str,
    port: int,
    api_version: float,
    remote_reset: bool,
    pin_imports: Dict[str, str],
    use_personal_branch: bool,
    concurrency: int,
) -> None:
    # Don't trust env to ignore .netrc credentials
    async_client = httpx.AsyncClient(trust_env=False)
    try:
        client = LookerClient(
            async_client, base_url, client_id, client_secret, port, api_version
        )
        runner = Runner(client, project, remote_reset, pin_imports, use_personal_branch)

        results = await runner.validate_data_tests(ref, filters, concurrency)
    finally:
        await async_client.aclose()

    for test in sorted(results["tested"], key=lambda x: (x["model"], x["explore"])):
        message = f"{test['model']}.{test['explore']}"
        printer.print_validation_result(status=test["status"], source=message)

    errors = sorted(
        results["errors"],
        key=lambda x: (
            x["model"] or "",
            x["explore"] or "",
            x["metadata"]["test_name"] or "",
        ),
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
async def run_sql(
    log_dir: str,
    project: str,
    ref: str,
    filters: List[str],
    base_url: str,
    client_id: str,
    client_secret: str,
    port: int,
    api_version: float,
    fail_fast: bool,
    incremental: bool,
    target: str,
    remote_reset: bool,
    concurrency: int,
    profile: bool,
    runtime_threshold: int,
    chunk_size: int,
    pin_imports: Dict[str, str],
    use_personal_branch: bool,
    ignore_hidden: bool,
    result_format: str,
) -> None:
    """Runs and validates the SQL for each selected LookML dimension."""
    # Don't trust env to ignore .netrc credentials
    async_client = httpx.AsyncClient(trust_env=False)
    try:
        client = LookerClient(
            async_client, base_url, client_id, client_secret, port, api_version
        )
        runner = Runner(client, project, remote_reset, pin_imports, use_personal_branch)

        results = await runner.validate_sql(
            ref,
            filters,
            fail_fast,
            incremental,
            target,
            concurrency,
            profile,
            runtime_threshold,
            chunk_size,
            ignore_hidden,
            result_format,
        )
    finally:
        await async_client.aclose()

    for test in sorted(results["tested"], key=lambda x: (x["model"], x["explore"])):
        message = f"{test['model']}.{test['explore']}"
        printer.print_validation_result(
            status=test["status"], skip_reason=test.get("skip_reason"), source=message
        )

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
