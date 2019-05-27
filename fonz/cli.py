import click
from fonz.connection import Fonz


@click.group()
def cli():
    pass


@click.command()
@click.argument('url', envvar='LOOKER_BASE_URL')
@click.argument('client_id', envvar='LOOKER_CLIENT_ID')
@click.argument('client_secret', envvar='LOOKER_CLIENT_SECRET')
@click.option('--port', default=19999)
@click.option('--api', default='3.0')
def connect(url, client_id, client_secret, port, api):
    client = Fonz(url, client_id, client_secret, port, api)
    client.connect()


@click.command()
@click.argument('project')
@click.argument('branch')
@click.argument('url', envvar='LOOKER_BASE_URL')
@click.argument('client_id', envvar='LOOKER_CLIENT_ID')
@click.argument('client_secret', envvar='LOOKER_CLIENT_SECRET')
@click.option('--port', default=19999)
@click.option('--api', default='3.0')
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

if __name__ == '__main__':
    cli()
