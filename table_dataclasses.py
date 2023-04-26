import uuid
from datetime import date, datetime
from dataclasses import dataclass, field


@dataclass
class IdField:
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    def get_fields(self):
        return self.__dict__['__match_args__']

@dataclass
class CreatedField:
    created_at: datetime = field(default=datetime.now())

@dataclass
class TimeCreateFields(CreatedField):
    updated_at: datetime = field(default=datetime.now())

@dataclass
class FilmWork(IdField, TimeCreateFields):
    title: str = field(default='')
    description: str = field(default=None)
    creation_date: date = field(default=None)
    rating: float = field(default=0.0)
    type: str = field(default=None)

@dataclass
class Person(IdField, TimeCreateFields):
    full_name: str = field(default='')


@dataclass
class Genre(IdField, TimeCreateFields):
    name: str = field(default='')
    description: str = field(default=None)


@dataclass
class GenreFilmWork(IdField, CreatedField):
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    genre_id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class PersonFilmWork(IdField, CreatedField):
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    person_id: uuid.UUID = field(default_factory=uuid.uuid4)
    role: str = field(default=None)


class TableData:
    decrypter: dict = {'created_at': 'created',
                       'updated_at': 'modified'}

    def __init__(self, table_dataclass, table: str = ''):
        self.table = table
        self._query_select: str = table
        self.table_dataclass = table_dataclass
        self.columns_table: tuple = self.table_dataclass.get_fields(table_dataclass)
        self.fields_for_insert: tuple = self._translating_fields()

    @property
    def query_select(self) -> str:
        return f"SELECT {', '.join(self.columns_table)} FROM {self.table}"
    
    @query_select.setter
    def query_select(self, arg: str):
        self._query_select = arg

    def _translating_fields(self):
        res = []
        for field in self.columns_table:
            if field in self.decrypter:
                res.append(self.decrypter[field])
            else:
                res.append(field)
        return tuple(res)

    def __str__(self):
        return f'TableData object: {self.table_dataclass}'


class TablesDB:
    def __init__(self):
        self.film_work: TableData = TableData(table="film_work", 
                                            table_dataclass=FilmWork)
        self.person: TableData = TableData(table="person", 
                                           table_dataclass=Person)
        self.genre: TableData = TableData(table="genre", 
                                          table_dataclass=Genre)
        self.person_film_work: TableData = TableData(table="person_film_work", 
                                                     table_dataclass=PersonFilmWork)
        self.genre_film_work: TableData = TableData(table="genre_film_work", 
                                                    table_dataclass=GenreFilmWork)
        self._attrs = tuple(self.__dict__)
        self.schema_db = "content"

    def __getitem__(self, index):
        return self.__dict__[self._attrs[index]]

    def __iter__(self):
        self._iterindex = 0
        return self
    
    def __next__(self):
        if self._iterindex < len(self._attrs):
            res = self[self._iterindex]
            self._iterindex += 1
            return res
        else:
            raise StopIteration
