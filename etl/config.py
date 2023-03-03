import yaml

from data_classes import AppSettings, ESSettings, PGSettings

with open('settings/config.yaml', "r") as f:
        config = yaml.safe_load(f)
        app_config = AppSettings.parse_obj(config['app'])
        pg_config = PGSettings.parse_obj(config['postgres'])
        es_config = ESSettings.parse_obj(config['elasticsearch'])
        dsl = {
                'dbname': pg_config.DB_NAME,
                'user': pg_config.POSTGRES_USER,
                'password': pg_config.POSTGRES_PASSWORD,
                'host': pg_config.DB_HOST,
                'port': pg_config.DB_PORT,
                'options': pg_config.options
        }
        movie_idx = es_config.movie_idx
        genre_idx = es_config.genre_idx
        person_idx = es_config.person_idx

