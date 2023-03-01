import yaml

from data_classes import AppSettings, ESSettings, PGSettings

with open('settings/config.yaml', "r") as f:
        config = yaml.safe_load(f)
        app_config = AppSettings.parse_obj(config['app'])
        pg_config = PGSettings.parse_obj(config['postgres'])
        es_config = ESSettings.parse_obj(config['elasticsearch'])

