from collections import namedtuple
from typing import Any, List, Tuple

from data_loader import utils

Column = namedtuple("Column", "column_name width datatype")


def safe_query(func):
    def func_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            utils.log(f"Problem with database: {e}", level="ERROR")
            return None

    return func_wrapper


def insert_into_table(connection, table_name: str, records: Tuple[Tuple[Any]]):
    with connection.cursor() as cursor:
        query = f"INSERT INTO {table_name} VALUES (%s, %s, %s)"
        cursor.executemany(query, records)
    connection.commit()


@safe_query
def select_all_from_table(connection, table_name: str):
    with connection.cursor() as cursor:
        sql = f"""SELECT * FROM {table_name}"""
        cursor.execute(sql)
        result = cursor.fetchall()
        return result


def create_table(connection, table_name: str, columns: List[Column]):
    with connection.cursor() as cursor:
        sql = f"""
            CREATE TABLE {table_name} (
            {stringify(columns)}
            )"""
        cursor.execute(sql)
    connection.commit()


def stringify(columns: List[Column]) -> str:
    result = []
    for name, width, datatype in columns:
        result.append(f"{name} {datatype}")
    return ", ".join(result)


@safe_query
def show_tables(connection):
    with connection.cursor() as cursor:
        sql = "SHOW TABLES"
        cursor.execute(sql)
        result = cursor.fetchall()
        return result
