import click
from connection import Fonz


@click.group()
def cli():
    pass


@click.command()
@click.argument('project')
@click.argument('branch')
@click.argument('base_url', envvar='LOOKER_BASE_URL')
@click.argument('client_id', envvar='LOOKER_CLIENT_ID')
@click.argument('client_secret', envvar='LOOKER_CLIENT_SECRET')
@click.option('--port', default=19999)
@click.option('--api', default='3.0')
def connect(base_url, client_id, client_secret, project, branch, port, api):
	client = Fonz(base_url, client_id, client_secret, project, branch, port, api)
	client.connect()

cli.add_command(connect)

if __name__ == '__main__':
	cli()