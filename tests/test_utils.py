import os
import csv
import pytest
from data_loader import utils, database_utils


@pytest.fixture(scope="session")
def specification():
    csv_data = [
        ["column _name", "width", "datatype"],
        ["name", "10", "TEXT"],
        ["valid", "1", "BOOLEAN"],
        ["count", "3", "INTEGER"],
    ]
    filename = "specs/testformatter2.csv"
    with open(filename, "w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(csv_data)
    csv_file.close()
    yield filename, csv_data
    os.remove(filename)


@pytest.fixture(scope="session")
def data_creator():
    data = ["Foonyoy   1  1\n", "Barzane   0 12\n", "Quuxitude 1103\n"]
    filename = "data/testformatter2.txt"
    with open(filename, "w") as file:
        for elem in data:
            file.write(elem)
    yield filename, data
    os.remove(filename)


def test_get_specification(specification):
    filename, data = specification
    spec = utils.get_specification(filename)
    assert spec == [database_utils.Column(*row) for row in data[1:]]


def test_get_data(data_creator):
    filename, data = data_creator
    spec = utils.get_specification(filename)
    records = utils.get_data(filename, spec)
    assert records == tuple([utils.read_line(row, spec) for row in data])


@pytest.mark.usefixtures("connection")
def test_create_table(connection, specification):
    filename, spec_data = specification
    spec = utils.get_specification(filename)
    tablename = utils.process_filename(filename)
    utils.create_table(connection, tablename, spec)
    tables = [
        table
        for result in database_utils.show_tables(connection)
        for table in result.values()
    ]
    assert tablename in tables


@pytest.mark.usefixtures("connection")
def test_insert_data_into_table(connection, data_creator):
    filename, data = data_creator
    spec = utils.get_specification(filename)
    tablename = utils.process_filename(filename)

    to_insert = tuple([utils.read_line(row, spec) for row in data])
    utils.insert_data_into_table(connection, tablename, to_insert)

    records = database_utils.select_all_from_table(connection, tablename)
    assert len(records) == len(to_insert)
