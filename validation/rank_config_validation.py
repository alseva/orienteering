from pathlib import Path

from constants import RANK_CONFIG_FILE
from validation.exceptions import AppException


def check_rank_config_exists():
    if not Path(RANK_CONFIG_FILE).exists():
        raise AppException('Excel file with rank configuration was not found.')
