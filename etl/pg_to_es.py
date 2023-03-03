import logging
import time

from config import es_config, app_config, movie_idx, genre_idx, person_idx
from db_operations import ElstcsrchOperations, PstgrsOperations
from transform import transformer

logging.basicConfig(filename=app_config.log_path, level=logging.DEBUG)

def updater(pg, es):
    for data in pg.pop_next_to_update():
        es.add_to_pack(*transformer(data))
        if len(es.pack) >= es_config.bulk_factor:
            es.last_time = pg.last_time
            es.load_data()
    es.last_time = pg.last_time
    es.load_data()


def сontinious_interaction():
    es = ElstcsrchOperations()
    pg = PstgrsOperations()

    idx_files = {
        movie_idx: es_config.movies_schema,
        genre_idx: es_config.genres_schema,
        person_idx: es_config.persons_schema,

    }

    for i, f in idx_files.items():
        if not es.check_idx(i):
            result = es.create_idxs(i, f)
            logging.info(f'Created index: {result}')

    while True:
        while pg.is_not_complete():
            pg.get_films_ids()
            updater(pg=pg, es=es)
            pg.check_persons_updates()
            updater(pg=pg, es=es)
            pg.check_genres_updates()
            updater(pg=pg, es=es)

        logging.info(f'All files are up to date! Waiting {app_config.await_time} sec')
        time.sleep(app_config.await_time)
        pg.tables = {'filmworks': True, 'genres': True, 'persons': True}

if __name__ == '__main__':
    сontinious_interaction()