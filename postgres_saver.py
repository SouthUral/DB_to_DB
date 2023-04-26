import psycopg2
from psycopg2.extras import DictCursor
from log_pack import log_error, log_success
from sqlite_extractor import SQLiteExtractor
from table_dataclasses import TablesDB, TableData
from contextlib import contextmanager

class PostgresSaver:
    '''Класс для загрузки данных в PostgreSQL'''

    def __init__(self, extractor: SQLiteExtractor, connection: psycopg2.connect, db_tables: TablesDB, chunk: int=20):
        self.extractor = extractor
        self.connection = connection
        self.curs = connection.cursor()
        self.db_tables = db_tables
        self.chunk = chunk

    def _slicer(self, data: list):
        for item in range(0, len(data), self.chunk):
            yield data[item :item  + self.chunk]
    
    def _request_creator(self, table_data: TableData, data_slice: list[dict]):
        columns = table_data.columns_table
        request_fields = ', '.join(table_data.fields_for_insert)
        data = []
        for row in data_slice:
            row_data = self.curs.mogrify(", ".join(['%s' for _ in columns]),[row.__dict__[column] for column in columns]).decode()
            row_data = row_data.replace("'None'", "null")
            row_data = f"({row_data})"
            data.append(row_data)
        data = ', '.join(data)
        request_insert = f'''INSERT INTO {self.db_tables.schema_db}.{table_data.table} ({request_fields}) 
            VALUES {data}
            ON CONFLICT (id) DO NOTHING;'''
        return request_insert

    def _data_recorder(self, table_data: TableData, data: list):
        slices = self._slicer(data)
        for slice in slices:
            request = self._request_creator(table_data, slice)
            try:
                self.curs.execute(request)
                self.connection.commit()
            except psycopg2.Error as err:
                log_error(f'Data recording error: {err}')
                raise SystemExit

    def save_all_data(self):
        '''Метод запускает алгоритм считывания и записи данных'''
        for table in self.db_tables:
            generator = self.extractor(table)
            for data in generator:
                self._data_recorder(table, data)
        log_success('All data is recorded')
    
    @staticmethod
    @contextmanager
    def conn_context(dsl: dict, cursor_factory: DictCursor):
        try:
            conn = psycopg2.connect(**dsl, cursor_factory=cursor_factory)
            log_success('Connect to the postgresql database')
        except psycopg2.OperationalError as err:
            log_error(err)
            raise SystemExit
        yield conn
        conn.close()
        log_success('Connection to postgresql database is closed')

