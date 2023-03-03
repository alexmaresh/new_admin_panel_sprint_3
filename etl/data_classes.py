from dataclasses import dataclass
from datetime import datetime
from pydantic import BaseModel


class AppSettings(BaseModel):
    backoff_start_sleep_time: float
    backoff_factor: int
    backoff_border_sleep_time: int
    await_time: float
    storage_file_path: str
    log_path: str


class ESSettings(BaseModel):
    ES_URL: str
    ES_PORT: int
    movie_idx: str
    genre_idx: str
    person_idx: str
    bulk_factor: int
    movies_schema: str
    genres_schema: str
    persons_schema: str


class PGSettings(BaseModel):
    DB_NAME: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    DB_HOST: str
    DB_PORT: int
    options: str
    bulk_factor: int
    limit_query: int


@dataclass
class MovieRaw:
    __slots__ = (
        'uuid', 'title', 'description', 'imdb_rating', 'role', 'person_id', 'person_name', 'genre', 'genre_id'
    )
    uuid: str
    title: str
    description: str
    imdb_rating: float
    role: str
    person_id: str
    person_name: str
    genre: str
    genre_id: str


@dataclass
class GenreRaw:
    __slots__ = (
        'uuid', 'name', 'description', 'updated_at'
    )
    uuid: str
    name: str
    description: str
    updated_at: str


@dataclass
class PersonRaw:
    __slots__ = (
        'uuid', 'full_name', 'birthday', 'updated_at'
    )
    uuid: str
    full_name: str
    birthday: str
    updated_at: datetime.date
