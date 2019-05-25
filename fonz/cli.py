import click
from connection import Fonz


@click.group()
def cli():
    pass


@click.command()
@click.argument('project')
@click.argument('branch')
@click.argument('url', envvar='LOOKER_BASE_URL')
@click.argument('client_id', envvar='LOOKER_CLIENT_ID')
@click.argument('client_secret', envvar='LOOKER_CLIENT_SECRET')
@click.option('--port', default=19999)
@click.option('--api', default='3.0')
def connect(url, client_id, client_secret, project, branch, port, api):
    client = Fonz(url, client_id, client_secret, project, branch, port, api)
    client.connect()
    explores = client.get_explores()
    explores = client.get_dimensions(explores)
    validate = client.validate_explores(explores)


cli.add_command(connect)

if __name__ == '__main__':
    cli()
