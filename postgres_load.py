import os
import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from contextlib import contextmanager

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


class Postgres:
    def __init__(self, ext_pg_conf: PgConfig, save_pg_conf: PgConfig):
        self.ext_pg_conf = ext_pg_conf
        self.save_pg_conf = save_pg_conf

    def work_Data(self):




class PostgresExtractor(pg_context):
    def __init__(self):
        pass


class PostgresSaver(pg_context):
    def __init__(self):
        pass


class PgConfig:
    def __init__(self, dbname: str, user: str, password: str, host: str, port: str):
        self.dbname = os.getenv(dbname)
        self.user = os.getenv(user)
        self.password = os.getenv(password)
        self.host = os.getenv(host)
        self.port = os.getenv(port)


