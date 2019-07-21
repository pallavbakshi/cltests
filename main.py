import click
import yaml

from data_loader import utils


@click.command()
@click.option("--config", default="config.yml", help="Config file")
@click.option(
    "--filename",
    help="Filename to be uploaded to db. No need to specify the path or the extension.",
)
def main(config, filename):
    with open(config, "r") as file:
        cnf = yaml.safe_load(file)
    connection = utils.get_connection(cnf, "clover")
    spec = utils.get_specification(filename)
    data = utils.get_data(filename, spec)

    table_name = utils.create_table_from_specification(connection, filename, spec)
    utils.insert_data_into_table(connection, table_name, data)
    utils.log(f"Data inserted into {table_name}", level="INFO")


if __name__ == "__main__":
    main()
