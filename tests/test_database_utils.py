import pytest
import yaml
from data_loader import utils
from data_loader import database_utils


@pytest.fixture(scope="session")
def connection(config="config.yml"):
    with open(config, "r") as file:
        cnf = yaml.safe_load(file)
    connection = utils.get_connection(cnf, mysql="clovertest")
    yield connection
    print("Tearing down")
    connection.cursor().execute("DROP DATABASE TEST")
    connection.cursor().execute("CREATE DATABASE TEST")
    connection.commit()
    connection.close()


@pytest.fixture
def dummy_table(connection):
    columns = [
        database_utils.Column("name", str(10), "TEXT"),
        database_utils.Column("valid", None, "BOOLEAN"),
        database_utils.Column("count", str(3), "INTEGER"),
    ]
    table_name = "dummy_table"
    database_utils.create_table(connection, table_name, columns)
    yield table_name
    connection.cursor().execute(f"DROP TABLE {table_name}")
    connection.commit()


def test_create_table(connection):
    columns = [
        database_utils.Column("name", str(10), "TEXT"),
        database_utils.Column("valid", None, "BOOLEAN"),
    ]
    database_utils.create_table(connection, "testformat99", columns)
    tables = [
        table
        for result in database_utils.show_tables(connection)
        for table in result.values()
    ]
    assert "testformat99" in tables


def test_insertion_into_table(connection, dummy_table):
    data = (("Clover", 1, 231), ("Clover", 1, 232))
    database_utils.insert_into_table(connection, dummy_table, data)
    from_database = database_utils.select_all_from_table(connection, dummy_table)
    from_database = tuple(tuple(record.values()) for record in from_database)
    assert data == from_database
