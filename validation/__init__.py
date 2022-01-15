from validation.app_config_validation import check_app_config_exists
from validation.rank_config_validation import check_rank_config_exists


def do_validation():
    check_app_config_exists()
    check_rank_config_exists()
