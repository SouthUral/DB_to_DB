import os
import sqlite3
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from postgres_saver import PostgresSaver
from sqlite_extractor import SQLiteExtractor
from table_dataclasses import *


def load_from_sqlite(sqlite_conn: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    data_tables = TablesDB()
    SQLextractor = SQLiteExtractor(sqlite_conn)
    postgres_saver = PostgresSaver(connection=pg_conn, db_tables=data_tables, extractor=SQLextractor)
    postgres_saver.save_all_data()


if __name__ == '__main__':
    load_dotenv()

    db_path = os.getenv('DBPATH_SQL')

    dsl = {
    'dbname': os.getenv('DBNAME_PG'),
    'user': os.getenv('USER_PG'),
    'password': os.getenv('PASSWORD_PG'),
    'host': os.getenv('HOST_PG'),
    'port': os.getenv('PORT_PG')
    }

    
    with SQLiteExtractor.conn_context(db_path) as sqlite_conn, PostgresSaver.conn_context(dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
