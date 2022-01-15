from errors import Error
from validation.app_config_validation import check_app_config
from validation.rank_config_validation import check_rank_config_exists


def do_validation():
    check_app_config()
    check_rank_config_exists()
    raise Error
