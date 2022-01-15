from pathlib import Path

from constants import RANK_CONFIG_FILE
from errors import Error


def check_rank_config_exists():
    if not Path(RANK_CONFIG_FILE).exists():
        raise Error('Excel file with rank configuration was not found.')
