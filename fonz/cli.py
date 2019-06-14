import sys
import click
import yaml
from fonz.connection import Fonz
from fonz.exceptions import SqlError, FonzException, ValidationError
from fonz.logger import GLOBAL_LOGGER as logger, LOG_FILEPATH


def handle_exceptions(function):
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except ValidationError as error:
            logger.error(f"{error}\n")
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
                f'Encountered unexpected error: "{error}"\n'
                f"Full error traceback logged to {LOG_FILEPATH}\n\n"
                "For support, please create an issue at "
                "https://github.com/dbanalyticsco/Fonz/issues\n"
            )
            sys.exit(1)

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
@click.option("--batch/--no-batch", default=False)
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


cli.add_command(connect)
cli.add_command(sql)

if __name__ == "__main__":
    cli()
