import logging

import pandas as pd

from app_config import ApplicationConfig
from logger import setup_logging
from rank_formula_config import RankFormulaConfig


def main():
    application_config = ApplicationConfig('Конфигуратор приложения.xlsx')
    rank_formula_config = RankFormulaConfig()
    protocols_df = load_protocols(application_config)
    current_rank_df = calculate_current_rank(rank_formula_config, protocols_df)
    save_current_rank(application_config, current_rank_df)


def load_protocols(application_config: ApplicationConfig) -> pd.DataFrame:
    return pd.DataFrame()


def calculate_current_rank(rank_formula_config: RankFormulaConfig, protocols_df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame()


def save_current_rank(application_config: ApplicationConfig, current_rank_df: pd.DataFrame):
    pass


if __name__ == '__main__':
    setup_logging()
    try:
        main()
    except Exception as e:
        logging.exception(e)
