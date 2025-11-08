import click


@click.group()
def cli():
    pass


@cli.command()
def hello():
    print("Hello from bike-share-ml!")


if __name__ == '__main__':
    cli()
