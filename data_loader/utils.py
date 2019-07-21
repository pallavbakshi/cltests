import sys
import pymysql
import csv
import glob
from typing import Any, List, Tuple

from .database_utils import Column, create_table, insert_into_table


def log(message, level="INFO"):
    print(f"{level}: {message}")


def get_connection(cnf, mysql):
    connection = pymysql.connect(
        host=cnf[mysql]["host"],
        user=cnf[mysql]["user"],
        port=cnf[mysql]["port"],
        password=cnf[mysql]["passwd"],
        db=cnf[mysql]["db"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    return connection


def get_specification(name: str) -> List[Column]:
    file_path = glob.glob(f"specs/{process_filename(name)}.csv")
    if not file_path:
        log(f"No spec file", level="WARNING")
        sys.exit(0)

    result = []
    with open(file_path[0]) as file:
        csv_reader = csv.reader(file, delimiter=",")
        next(csv_reader, None)
        for row in csv_reader:
            result.append(Column(*row))
    return result


def get_data(name: str, spec: List[Column]) -> Tuple[Tuple[Any]]:
    file_path = glob.glob(f"data/{process_filename(name)}*.txt")
    if not file_path:
        log(f"No data file", level="WARNING")
        sys.exit(0)

    log(f"Found files: {file_path}. Getting data from first file {file_path[0]}")
    with open(file_path[0], "r") as file:
        content = file.readlines()
    return tuple(read_line(row, spec) for row in content)


def read_line(row: str, spec: List[Column]) -> Tuple[Tuple[Any]]:
    char_pointer = 0
    result = []
    for name, width, datatype in spec:
        width = int(width)
        start, end = char_pointer, char_pointer + width
        result.append(row[start:end])
        char_pointer += width
    return tuple(result)


def create_table_from_specification(connection, name: str, spec: List[Column]) -> None:
    try:
        create_table(connection, process_filename(name), spec)
        return process_filename(name)
    except Exception as e:
        log(f"Table cannot be created: {e}", level="ERROR")


def insert_data_into_table(connection, name: str, data: Tuple[Tuple[Any]]) -> None:
    try:
        insert_into_table(connection, name, data)
    except Exception as e:
        log(f"Data cannot be inserted: {e}", level="ERROR")


def process_filename(file: str) -> str:
    result = file.split("/")
    result = result[-1].split(".")[0]
    return result
