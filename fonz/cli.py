import sys
import click
import yaml
from fonz.connection import Fonz
from fonz.exceptions import SqlError, FonzException
from fonz.printer import print_start, print_pass, print_fail, print_error, print_stats
from fonz.logger import GLOBAL_LOGGER as logger, LOG_FILEPATH


def handle_exceptions(function):
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except FonzException as error:
            logger.error(
                f"{error}\n\n"
                "For support, please create an issue at "
                "https://github.com/dbanalyticsco/Fonz/issues\n"
            )
        except Exception as error:
            logger.debug(error, exc_info=True)
            logger.error(
                f'Encountered unexpected error: "{error}"\n'
                f"Full error traceback logged to {LOG_FILEPATH}\n\n"
                "For support, please create an issue at "
                "https://github.com/dbanalyticsco/Fonz/issues\n"
            )

    return wrapper


class CommandWithConfig(click.Command):
    @handle_exceptions
    def invoke(self, ctx):
        config_filename = ctx.params.get("config_file")
        if config_filename is not None:
            with open(config_filename) as file:
                config = yaml.safe_load(file)
                for param, value in ctx.params.items():
                    if value is None and param in config:
                        ctx.params[param] = config[param]

        return super(CommandWithConfig, self).invoke(ctx)


@click.group()
def cli():
    pass


@click.command(cls=CommandWithConfig)
@click.option("--base-url", envvar="LOOKER_BASE_URL")
@click.option("--client-id", envvar="LOOKER_CLIENT_ID")
@click.option("--client-secret", envvar="LOOKER_CLIENT_SECRET")
@click.option("--config-file")
@click.option("--port", default=19999)
@click.option("--api-version", default="3.0")
def connect(base_url, client_id, client_secret, config_file, port, api_version):
    client = Fonz(base_url, client_id, client_secret, port, api_version)
    client.connect()


@click.command(cls=CommandWithConfig)
@click.option("--project", envvar="LOOKER_PROJECT")
@click.option("--branch", envvar="LOOKER_GIT_BRANCH")
@click.option("--base-url", envvar="LOOKER_BASE_URL")
@click.option("--client-id", envvar="LOOKER_CLIENT_ID")
@click.option("--client-secret", envvar="LOOKER_CLIENT_SECRET")
@click.option("--config-file")
@click.option("--port", default=19999)
@click.option("--api-version", default="3.0")
def sql(
    project, branch, base_url, client_id, client_secret, config_file, port, api_version
):
    client = Fonz(
        base_url, client_id, client_secret, port, api_version, project, branch
    )
    client.connect()
    client.update_session()
    explores = client.get_explores()

    explore_count = len(explores)
    for index, explore in enumerate(explores):
        model = explore["model"]
        explore_name = explore["explore"]
        dimensions = client.get_dimensions(model, explore_name)

        print_start(explore_name, index + 1, explore_count)

        try:
            client.validate_explore(model, explore_name, dimensions)
        except SqlError as error:
            client.handle_sql_error(error.query_id, error.message, error.explore_name)
            print_fail(explore_name, index + 1, explore_count)
        else:
            print_pass(explore_name, index + 1, explore_count)

        errors = 0

    for message in client.messages:
        errors += 1
        print_error(message)
    print_stats(errors, explore_count)
    if errors > 0:
        sys.exit(1)


cli.add_command(connect)
cli.add_command(sql)

if __name__ == "__main__":
    cli()
