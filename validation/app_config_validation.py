from pathlib import Path

from constants import APP_CONFIG_FILE
from validation.exceptions import AppException


def check_app_config_exists():
    if not Path(APP_CONFIG_FILE).exists():
        raise AppException('Excel file with application configuration was not found.')
