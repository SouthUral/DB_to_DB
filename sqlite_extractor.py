import sqlite3

from contextlib import contextmanager
from table_dataclasses import TableData
from log_pack import log_error, log_success


class SQLiteExtractor:
    '''Класс для выгрузки данных из SQLite, данные выгружаются чанками'''
    def __init__(self, connection: sqlite3.Connection, chunk: int=500):
        self.curs = connection.cursor()
        self.chunk: int = chunk

    @staticmethod
    def dict_factory(cursor, row):
        columns = [column[0] for column in cursor.description]
        return {key: value for key, value in zip(columns, row)}

    @staticmethod
    @contextmanager
    def conn_context(db_path: str):
        try:
            conn = sqlite3.connect(db_path)
            log_success('Connect to the sqlite database')
        except sqlite3.Error as err:
            log_error(err)
            raise SystemExit
        conn.row_factory = SQLiteExtractor.dict_factory
        yield conn
        conn.close()
        log_success('Connection to sqlite database is closed')

    def __call__(self, table_data: TableData):
        last_id = None
        while True:
            query = table_data.query_select
            params: list = []
            if last_id is not None:
                query += " WHERE id > ?"
                params.append(last_id)

            query += " ORDER BY id LIMIT ?;"
            params.append(self.chunk)
            try:
                self.curs.execute(query, tuple(params))
            except Exception as err:
                log_error(err)
                raise SystemExit
            data = [table_data.table_dataclass(**line) for line in self.curs.fetchall()]

            if not data:
                break
            yield data
            
            last_id = data[-1].id
