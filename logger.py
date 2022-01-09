import logging.config
from pathlib import Path

import yaml


def setup_logging():
    logs_dir = Path('./logs')
    logs_dir.mkdir(exist_ok=True)
    config_file = Path('logger.yaml')
    with config_file.open() as stream:
        config = yaml.safe_load(stream)
    logging.config.dictConfig(config)
