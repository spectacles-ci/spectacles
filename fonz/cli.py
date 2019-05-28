import click
import yaml
from fonz.connection import Fonz


class CommandWithConfig(click.Command):
    def invoke(self, ctx):
        click.echo(ctx.params)
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
@click.option("--api", default="3.0")
def connect(base_url, client_id, client_secret, config_file, port, api):
    client = Fonz(base_url, client_id, client_secret, port, api)
    client.connect()


@click.command()
@click.argument("project")
@click.argument("branch")
@click.argument("url", envvar="LOOKER_BASE_URL")
@click.argument("client_id", envvar="LOOKER_CLIENT_ID")
@click.argument("client_secret", envvar="LOOKER_CLIENT_SECRET")
@click.option("--port", default=19999)
@click.option("--api", default="3.0")
def sql(url, client_id, client_secret, port, api, project, branch):
    client = Fonz(url, client_id, client_secret, port, api, project, branch)
    client.connect()
    client.update_session()
    explores = client.get_explores()
    explores = client.get_dimensions(explores)
    validate = client.validate_explores(explores)
    client.print_results(validate)


cli.add_command(connect)
cli.add_command(sql)

if __name__ == "__main__":
    cli()
