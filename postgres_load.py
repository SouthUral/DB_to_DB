import os
import json
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
    count_partition: int
    Saver: PostgresSaver
    Extractor: PostgresExtrator

    def __init__(self, ext_pg_conf: PgConfig, save_pg_conf: PgConfig, chunk_extract=500, chunk_save=20):
        self.ext_pg_conf = ext_pg_conf
        self.save_pg_conf = save_pg_conf
        self.chunk_extract = chunk_extract
        self.chunk_save = chunk_save

    def _worker(self):
        '''Управляющий блок запускающий логику'''
        with self.conn_context(self.save_pg_conf(), cursor_factory=DictCursor) as save_conn, self.conn_context(self.ext_pg_conf(), cursor_factory=DictCursor) as ext_conn:
            self.Saver = PostgresSaver(save_conn, self.chunk_save)
            self.Extractor = PostgresExtrator(ext_conn, self.chunk_extract)
            # Запуск блока проверки
            if not self._check_db_partitions() and self.count_partition == 0:



            self.Extractor.check_id = Saver.chek_created_id()
            for data in Extractor.generator():
                Saver.data_recorder(data)
            log_success('All data is recorded')

    def _check_db_partitions(self) -> bool:
        '''Блок проверки'''
        self.data_for_partition = self.Extractor.get_distinct_object()
        self.counter_partiton = self.Saver.get_counter_partiton()
        return len(self.data_for_partition) == self.counter_partiton






class PostgresExtrator():
    '''Класс для взаимодействия с БД откуда считываются данные'''
    check_id: str

    def __init__(self, pg_conn, chunk: int):
        self.pg_conn = pg_conn
        self.cursor = pg_conn.cursor()
        self.chunk = chunk

    def generator_data(self):
        pass

    def get_distinct_object(self):
        '''Возвращает список с уникальным object_id'''
        self.cursor.execute('''
            SELECT DISTINCT ON (object_id)
            created_id,
            device_id,
            object_id,
            mes_id,
            mes_time,
            mes_code,
            mes_status,
            mes_data,
            event_value,
            event_data
            FROM device.messages;'''
            )
        return self.cursor.fetchall()


class PostgresSaver():
    '''Класс для взаимодействия с БД куда производится запись'''
    def __init__(self, pg_conn, chunk: int):
        self.pg_conn = pg_conn
        self.cursor = pg_conn.cursor()
        self.chunk = chunk

    def get_counter_partiton(self):
        '''Возвращает количество секций в таблице'''
        self.cursor.execute('''
            SELECT COUNT(table_name) 
            FROM information_schema.tables
            WHERE table_name LIKE 'message\_%';'''
            )
        return self.cursor.fetchone()

    def make_partition(self, data):
        '''Вызывает процедуру, которая разбивает таблицу на секции'''
        self.cursor.execute("CALL device.check_section($1, $2);", json.dumps(data), None)
        self.clean_data()
        self.pg_conn.commit()
        log_success('Sections have been created in the table device.messages')

    def clean_data(self):
        '''Очищает таблицу device.messages вместе с секциями'''
        self.cursor.execute("TRUNCATE TABLE device.messages")
        log_success('Table device.messages is cleared')
    

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


