import os
import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from contextlib import contextmanager
from log_pack import log_error, log_success


class pg_context:
    @staticmethod
    @contextmanager
    def conn_context(dsl: dict, cursor_factory: DictCursor):
        '''Контекстный менеджер, сделан для обработки ошибок'''
        try:
            conn = psycopg2.connect(**dsl, cursor_factory=cursor_factory)
            log_success('Connect to the postgresql database')
        except psycopg2.OperationalError as err:
            log_error(err)
            raise SystemExit
        yield conn
        conn.close()
        log_success('Connection to postgresql database is closed')


class Postgres(pg_context):
    def __init__(self, ext_pg_conf: PgConfig, save_pg_conf: PgConfig, chunk_extract=500, chunk_save=20):
        self.ext_pg_conf = ext_pg_conf
        self.save_pg_conf = save_pg_conf
        self.chunk_extract = chunk_extract
        self.chunk_save = chunk_save

    def _worker(self):
        with self.conn_context(self.save_pg_conf(), cursor_factory=DictCursor) as save_conn, self.conn_context(self.ext_pg_conf(), cursor_factory=DictCursor) as ext_conn:
            Saver = PostgresSaver(save_conn, self.chunk_save)
            Extractor = PostgresExtrator(ext_conn, self.chunk_extract)
            Extractor.check_id = Saver.chek_created_id()
            for data in Extractor.generator():
                Saver.data_recorder(data)
            log_success('All data is recorded')


class PostgresExtrator():
    check_id: str

    def __init__(self, pg_conn, chunk: int):
        self.pg_conn = pg_conn
        self.cursor = pg_conn.cursor()
        self.chunk = chunk

    def generator_data(self):
        pass


class PostgresSaver():
    def __init__(self, pg_conn, chunk: int):
        self.pg_conn = pg_conn
        self.cursor = pg_conn.cursor()
        self.chunk = chunk
    
    def data_recorder(self, data):
        pass

    def chek_created_id(self):
        self.cursor.execute('''
            SELECT m.created_id
            FROM device.messages m
            ORDER BY m.created_id DESC
            LIMIT 1;'''
            )
        return self.cursor.fetchone()



class PgConfig:
    def __init__(self, dbname: str, user: str, password: str, host: str, port: str):
        self.dbname = os.getenv(dbname)
        self.user = os.getenv(user)
        self.password = os.getenv(password)
        self.host = os.getenv(host)
        self.port = os.getenv(port)

    def __call__(self):
        return self.__dict__


