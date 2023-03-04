import logging

import psycopg2
from elasticsearch import Elasticsearch
from datetime import datetime as dt
from config import pg_config, dsl, es_config, movie_idx
from storage import State, JsonFileStorage
from settings import sql_queries
from backoff import backoff
import json


class PstgrsOperations:
    def __init__(self):
        self.connection = psycopg2.connect(**dsl)
        self.cursor = self.connection.cursor()
        self.state = State(JsonFileStorage())
        self.last_time = None
        self.rows = None
        self.tables = {'filmworks': True, 'genres': True, 'persons': True}

    @backoff()
    def get_data(self, query: str, params=None, size=10):
        logging.info("PstgrsOperations: get data")
        '''Get some data from PG: query, params and size'''
        if params is None:
            params = []
        if self.connection.close:
            self.connection = psycopg2.connect(**dsl)
            self.cursor = self.connection.cursor()
        self.cursor.execute(query, vars=params)
        if size:
            self.rows = self.cursor.fetchmany(size=size)
        else:
            self.rows = self.cursor.fetchall()

    def set_start_time(self):
        '''Set start time for tables: filmworks, genres, persons'''
        self.get_data(sql_queries.sql_get_top_time_person, size=1)
        p_time = self.rows[0][0]
        self.state.set_state('persons', p_time)
        self.get_data(sql_queries.sql_get_top_time_genre, size=1)
        g_time = self.rows[0][0]
        self.state.set_state('genres', g_time)
        self.last_time = dt.fromisoformat('1999-01-01 12:00:00.000001+00:00')
        self.state.set_state('filmworks', self.last_time)

    def is_not_complete(self):
        for _, v in self.tables.items():
            if v:
                return True
        return False

    def get_films_ids(self):
        if not self.state.get_state('filmworks'):
            self.set_start_time()
        self.last_time = dt.fromisoformat(self.state.get_state('filmworks'))
        self.get_data(sql_queries.sql_get_new_ids, params=[self.last_time, ], size=pg_config.bulk_factor)
        self.ids_to_update = self.rows
        self.tables['filmworks'] = len(self.rows) != 0

    def pop_next_to_update(self):
        while self.ids_to_update:
            ids, updated_at = self.ids_to_update.pop(0)
            self.get_data(sql_queries.sql_get_film, params=[ids, ])
            if self.last_time < updated_at:
                self.last_time = updated_at
            yield self.rows

    def _push(self, table_name):
        last_time = self.rows[-1][1]
        table_name_ids = tuple([x[0] for x in self.rows])
        if table_name == 'genres':
            self.get_data(sql_queries.sql_push_genres, params=(table_name_ids, pg_config.limit_query,))
        if table_name == 'persons':
            self.get_data(sql_queries.sql_push_persons, params=(table_name_ids, pg_config.limit_query,))
        self.ids_to_update = self.rows
        self.state.set_state(table_name, last_time)

    def check_persons_updates(self):
        persons_last_time = self.state.get_state('persons')
        self.get_data(sql_queries.sql_check_persons, params=(persons_last_time,))
        if len(self.rows) != 0:
            self._push('persons')
        self.tables['persons'] = len(self.rows) != 0

    def check_genres_updates(self):
        genres_last_time = self.state.get_state('genres')
        self.get_data(sql_queries.sql_check_genres, params=(genres_last_time,))

        if len(self.rows) != 0:
            self._push('genres')
        self.tables['genres'] = len(self.rows) != 0

    # На самом деле __del__ всегда вызывается по завершении работы интерпретатора.
    # Пруф: https://habr.com/ru/post/186608/comments/#comment_6492862
    def __del__(self):
        self.connection.close()


class ElstcsrchOperations:

    @backoff()
    def __init__(self):
        logging.info("ElstcsrchOperations: init")
        self.connection = Elasticsearch(host=es_config.ES_URL)
        self.connection.cluster.health(wait_for_status='yellow')
        self.last_time = None
        self.state = State(JsonFileStorage())
        self.pack = []

    def add_to_pack(self, doc: dict, uuid: str):
        index_row = {"index": {"_index": "movies", "_id": f"{uuid}"}}
        self.pack.append(json.dumps(index_row) + '\n' + json.dumps(doc))

    @backoff()
    def load_data(self):
        logging.info("ElstcsrchOperations: load data")
        if not self.pack:
            return
        body = '\n'.join(self.pack)
        res = self.connection.bulk(body=body, index=movie_idx, params={'filter_path': 'items.*.error'})
        if not res:
            logging.info("Pack of %s is added" % len(self.pack))
            self.pack.clear()
            self.state.set_state('filmworks', self.last_time)
        else:
            logging.error(res)

    def check_idx(self, idx):
        return self.connection.indices.exists(index=idx)

    def create_idxs(self, idx, file):
        with open(file, 'r') as f:
            self.connection.indices.create(index=idx, body=json.load(f))
            return self.connection.indices.get(index=idx)

    def __del__(self):
        self.connection.close()
