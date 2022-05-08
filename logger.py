import logging.config
import sys
from pathlib import Path

import yaml

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    # running in a bundle
    bundle_dir = Path(getattr(sys, '_MEIPASS'))
else:
    # running in a normal Python environment
    bundle_dir = Path(__file__).resolve().parent


def setup_logging():
    logs_dir = Path('./logs')
    logs_dir.mkdir(exist_ok=True)
    config_file = bundle_dir / 'logger.yaml'
    with config_file.open() as stream:
        config = yaml.safe_load(stream)
    logging.config.dictConfig(config)
