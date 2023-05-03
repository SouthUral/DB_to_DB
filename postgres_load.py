import os
import json
import time
import datetime
import psycopg2
from dotenv import load_dotenv
from statistics import mean
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor, execute_values
from contextlib import contextmanager
from log_pack import log_error, log_success


class JsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime.date, datetime.datetime, datetime.time)):
            return o.isoformat()
        if isinstance(o, datetime.timedelta):
            return o.total_seconds()
        return json.JSONEncoder.default(self, o)

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

    def __init__(self, ext_pg_conf, save_pg_conf, chunk_extract=10000, chunk_save=1000):
        self.ext_pg_conf = ext_pg_conf
        self.save_pg_conf = save_pg_conf
        self.chunk_extract = chunk_extract
        self.chunk_save = chunk_save
        self.start = 0
        self.end = 0

    def _worker(self):
        '''Управляющий блок запускающий логику'''
        with self.conn_context(self.save_pg_conf(), cursor_factory=DictCursor) as save_conn, self.conn_context(self.ext_pg_conf(), cursor_factory=DictCursor) as ext_conn:
            self.Saver = PostgresSaver(save_conn, self.chunk_save)
            self.Extractor = PostgresExtrator(ext_conn, self.chunk_extract)
            # Запуск блока проверки
            if not self._check_db_partitions() and self.counter_partiton == 0:
                self.Saver.make_partition(self.data_for_partition)
                if self.Saver.get_counter_partiton() == 0:
                    # если после этих действий секции не создались то вызывается исключение
                    log_error("sections were not created, check the script operation")
                    raise SystemExit

            self.Extractor.check_id = self.Saver.chek_created_id()
            timer = []
            self.start = time.perf_counter()
            rows_DB_1 = self.Extractor.get_count_rows()
            rows_DB_2 = self.Saver.get_count_rows()
            if rows_DB_2 != 0:
                counter = rows_DB_2
                log_success(f"Выполнено: {round(rows_DB_2 / rows_DB_1 * 100, 2)} %")
            else:
                counter = 0
            for data in self.Extractor.generator_data():
                count_rows = self.Saver.data_recorder(data)
                counter += count_rows
                percent = round(counter / rows_DB_1 * 100, 2)
                log_success(f"Выполнено: {percent} %")
                self.end = time.perf_counter()
                time_worker = (self.end - self.start) * ((rows_DB_1 - counter) / count_rows) / 60
                timer.append(time_worker)
                log_success(f"Примерное время выполнения скрипта: {round(mean(timer), 2)} минут")
                self.start = self.end
            log_success('All data is recorded')

    def _check_db_partitions(self) -> bool:
        '''Блок проверки'''
        self.data_for_partition = self.Extractor.get_distinct_object()
        self.counter_partiton = self.Saver.get_counter_partiton()
        return len(self.data_for_partition) == self.counter_partiton

    def __call__(self):
        self._worker()


class PostgresExtrator():
    '''Класс для взаимодействия с БД откуда считываются данные'''

    def __init__(self, pg_conn, chunk: int):
        self.pg_conn = pg_conn
        self.cursor = pg_conn.cursor()
        self.chunk = chunk
        self.check_id = 0

    def generator_data(self):
        while True:
            try:
                self.cursor.execute('''
                    SELECT 
                        dda.id,
                        dda.received_time as created_at,
                        convert_from(created_id, 'utf8') as created_id,
                        device_id,
                        object_id,
                        mes_id,
                        mes_time,
                        co.code as mes_code,
                        (dda."data" -> 'status_info') mes_status,
                        dda."data" as mes_data,
                        co.const_value as event_value,
                        event_data
                    FROM
                        sh_ilo.data_device_archive dda 
                        JOIN sh_data.v_constants co on co.code = dda.event and co.class in ('DBMSGTYPE', 'DBLOGICTYPE')
                    WHERE dda.id > %s::int8
                    ORDER BY dda.id
                    LIMIT %s::int4;''', (self.check_id, self.chunk))
            except Exception as err:
                log_error(err)
                raise SystemExit
            data = self.cursor.fetchall()
            if not data:
                break
            yield data
            self.check_id = int(data[-1]['id'])
            print(self.check_id)

    def get_distinct_object(self):
        '''Возвращает список с уникальным object_id'''
        self.cursor.execute('''
            SELECT DISTINCT ON (object_id) 
                object_id
            FROM
                sh_ilo.data_device_archive''')
        data = self.cursor.fetchall()
        return data

    def get_count_rows(self):
        self.cursor.execute('select COUNT(id) from sh_ilo.data_device_archive')
        data = int(self.cursor.fetchone()[0])
        return data



class PostgresSaver():
    '''Класс для взаимодействия с БД куда производится запись'''
    def __init__(self, pg_conn, chunk: int):
        self.pg_conn = pg_conn
        self.cursor = pg_conn.cursor()
        self.chunk = chunk

    def _slicer(self, data: list):
        '''Нарезает список данных на чанки для дальнейшей обработки'''
        for item in range(0, len(data), self.chunk):
            yield data[item :item  + self.chunk]

    def get_counter_partiton(self):
        '''Возвращает количество секций в таблице'''
        self.cursor.execute('''
            SELECT COUNT(table_name) 
            FROM information_schema.tables
            WHERE table_name LIKE 'message\_%';'''
            )
        return self.cursor.fetchone()[0]

    def make_partition(self, data):
        '''Вызывает процедуру, которая разбивает таблицу на секции'''
        for row in data:
            self.make_section(dict(row)['object_id'])
        self.pg_conn.commit()
        log_success('Sections have been created in the table device.messages')

    def make_section(self, number_section):
        '''Создает секции'''
        try:
            name_section = f"device.message_{number_section}"
            query = 'CREATE TABLE {} PARTITION OF device.messages FOR VALUES IN ({})'.format(name_section, number_section)
            self.cursor.execute(query)
            # self.cursor.execute('CREATE TABLE %s PARTITION OF device.messages FOR VALUES IN (%s)', (name_section, number_section))
        except Exception as err:
            log_error(err)
            raise SystemExit

    # def clean_data(self):
    #     '''Очищает таблицу device.messages вместе с секциями'''
    #     self.cursor.execute("TRUNCATE TABLE device.messages")
    #     log_success('Table device.messages is cleared')
    
    def data_recorder(self, data: list):
        '''Записывает данные чанками'''
        count_rows = 0
        for slice_data in self._slicer(data):
            serialize_rows = self.serialize_for_insert(slice_data)
            try:
                execute_values(self.cursor,
                    '''INSERT INTO device.messages (
                        offset_msg,
                        created_at,
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
                    )
                    VALUES %s''', serialize_rows)
            except Exception as err:
                log_error(err)
                raise SystemExit
            self.pg_conn.commit()
            count_rows += len(serialize_rows)
            log_success(f'{count_rows} rows are written to the table')
        return count_rows

    def serialize_for_insert(self, data):
        res_arr = []
        columns = (
                'id',
                'created_at',
                'created_id',
                'device_id',
                'object_id',
                'mes_id',
                'mes_time',
                'mes_code',
                'mes_status',
                'mes_data',
                'event_value',
                'event_data')
        for row in data:
            try:
                item_dict = dict(row)
                row_set = [json.dumps(item_dict[column], cls=JsonEncoder) for column in columns]
            except Exception as err:
                log_error(err)
                raise SystemExit
            res_arr.append(row_set)
        return res_arr

    def chek_created_id(self):
        self.cursor.execute('''
            SELECT offset_msg
            FROM device.messages
            ORDER BY offset_msg DESC
            LIMIT 1;'''
            )
        res = self.cursor.fetchone()
        return 0 if not res else int(res)

    def get_count_rows(self):
        self.cursor.execute('select COUNT(id) from device.messages')
        data = self.cursor.fetchone()[0]
        return 0 if not data else int(data)

class PgConfig:
    def __init__(self, dbname: str, user: str, password: str, host: str, port: str):
        self.dbname = os.getenv(dbname)
        self.user = os.getenv(user)
        self.password = os.getenv(password)
        self.host = os.getenv(host)
        self.port = os.getenv(port)

    def __call__(self):
        return self.__dict__


if __name__ == '__main__':
    load_save = Postgres(ext_pg_conf=PgConfig('DBNAME_1', 'USER_1', 'PASSWORD_1', 'HOST_1', 'PORT_1')
    , save_pg_conf=PgConfig('DBNAME_2', 'USER_2', 'PASSWORD_2', 'HOST_2', 'PORT_2'))
    load_save()
    