# DB_to_DB

Алгоритм кода:

 - Получение данных из переменных окружение
 - Передача данных экземпляру управляющего класса

 - Postgres: Запуск контекстного менеджера и передача данных о подключении, получение коннекторов для баз данных

Внутри контекстного менеджера:

 - PostgresSaver: Запрос в БД_2 для получения последнего created_id

 - Блок проверки: (просто делает проверку и отдает булево, что делать дальше с этим булевом решает контекст)
    - функция с проверкой количества секций:
        - если True запускается второй сценарий
        - если False запускается первый сценарий

Первый сценарий:

 - PostgresExtrator: запрос в котором получают по одному экземпляру каждой техники (только первые записи) (со всеми остальными данными)
   (select distinct on (object_id)
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
            from device.messages;)

 - Количество строк записывается в переменную Counter_section

 - Не определено: данные конвертируются в json

 - PostgresSaver: получение пакета данных с json строками, и итерация по пакету
    - в итерациях вызывается процедура postgres check_section(json, none)
    - после нужно удалить все данные, при этом в результирующей таблице DB_1 останутся все секции

    - Блок проверки: проверка на то что количество секций должно быть такое же как и количество строк в первом сценарии(
        select COUNT(table_name) from information_schema.tables
            where table_name LIKE 'message\_%';)
        - если строк меньше то выводится ошибка

Запуск второго сценария:
    Условия запуска второго сценария:
        - Блок проверки выдает True

    Проверка наличия записей в таблице:
        - Если переменная created_id пустая то:
            - Делается запрос к базе, чтобы получить created_id:
                ('''
                SELECT m.created_id
                FROM device.messages m
                ORDER BY m.created_id DESC
                LIMIT 1;''')
            - Переменная передается в PostgresExtrator (не важно пустая или нет)

    - PostgresExtrator: выполняется запрос( в зависимости от переменной created_id и chunk) (это генератор, полученные значения он возвращает)
        - Если created_id пустой то запрос забирает [:cunk] занчений из базы и в created_id записывает последнее
            sql скрипт (select 
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
                    from device.messages
                    where created_id > 'created_id'
                    order by created_id
                    limit cunk;)
        - Если created_id есть то запрос меняется и забирает [created_id: chunk] и переписывает created_id
        - Когда данные заканчиваются цикл прекращается


