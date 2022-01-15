import logging
import os
import re

import pandas as pd
from bs4 import BeautifulSoup

from app_config import ApplicationConfig
from logger import setup_logging
from rank_formula_config import RankFormulaConfig


def main():
    application_config = ApplicationConfig('Конфигуратор приложения.xlsx')
    rank_formula_config = RankFormulaConfig('Конфигуратор формулы ранга.xlsx')
    protocols_df = load_protocols(application_config, rank_formula_config)
    current_rank_df = calculate_current_rank(application_config, rank_formula_config, protocols_df)
    save_current_rank(application_config, current_rank_df)


def load_protocols(application_config: ApplicationConfig, rank_formula_config: RankFormulaConfig) -> pd.DataFrame:
    dfs_union = pd.DataFrame()
    df_not_started = pd.DataFrame()
    df_left_race = pd.DataFrame()

    # one iteration - one protocol -------------------------------------------------------------------------------------
    for name in os.listdir(application_config.protocols_dir):
        if os.path.isfile(os.path.join(application_config.protocols_dir, name)):
            dfs = pd.read_html(application_config.protocols_dir / name)

            soup = BeautifulSoup(open(application_config.protocols_dir / name, 'r'), 'lxml')

            heading2 = ['h2']
            heading1 = ['h1']

            # one iteration - one group --------------------------------------------------------------------------------
            for tbl in range(len(dfs)):
                header1 = str(soup.find_all(heading1)[0].text.strip())
                competition = re.split('\. ', header1)[-2].strip()
                competition_date = re.findall('[0-9]{2}\.[0-9]{2}\.[0-9]{4}', header1)[0]

                # Transform protocol columns ---------------------------------------------------------------------------
                dfs[tbl]['Возрастная группа'] = str(soup.find_all(heading2)[tbl].text.strip())
                dfs[tbl] = dfs[tbl].merge(
                    rank_formula_config.group_rank_df,
                    how='left',
                    on='Возрастная группа',
                    suffixes=(None, '_config'))
                dfs[tbl]['Соревнование'] = competition
                dfs[tbl]['Дата соревнования'] = competition_date
                dfs[tbl]['Фамилия'] = dfs[tbl]['Фамилия'].str.upper()
                dfs[tbl]['Имя'] = dfs[tbl]['Имя'].str.upper()

                dfs[tbl] = dfs[tbl].merge(application_config.mapping_yob_df,
                                          how='left',
                                          on=['Фамилия', 'Имя', 'Возрастная группа'],
                                          suffixes=(None, '_map'))
                dfs[tbl].loc[((dfs[tbl]['Г.р.'] == 0) | (dfs[tbl]['Г.р.'].isnull())), 'Г.р.'] = dfs[tbl].loc[
                    ((dfs[tbl]['Г.р.'] == 0) | (dfs[tbl]['Г.р.'].isnull())), 'Г.р._map']
                dfs[tbl].drop(labels='Г.р._map', axis=1, inplace=True)
                # Transform protocol columns ---------------------------------------------------------------------------

                df_not_started = df_not_started.append(dfs[tbl][dfs[tbl]['Результат'] == 'н/с'])
                df_left_race = df_left_race.append(dfs[tbl][dfs[tbl]['Результат'] == 'cнят'])

                # filter records without result
                dfs[tbl] = dfs[tbl][~((dfs[tbl]['Результат'] == 'cнят') | (dfs[tbl]['Результат'] == 'н/с'))]
                # filter records without result

                dfs[tbl]['Результат'] = pd.to_datetime(dfs[tbl]['Результат'])
                dfs[tbl]['result_in_seconds'] = (
                        dfs[tbl]['Результат'].dt.hour * 60 * 60 +
                        dfs[tbl]['Результат'].dt.minute * 60 +
                        dfs[tbl]['Результат'].dt.second)
                dfs[tbl]['Результат'] = dfs[tbl]['Результат'].dt.time

            dfs_union = dfs_union.append(pd.concat(dfs))
            dfs_union.to_excel(application_config.rank_dir / 'Протоколы.xlsx')
            df_not_started.to_excel(application_config.rank_dir / 'Протоколы_не_стартовали.xlsx')
            df_left_race.to_excel(application_config.rank_dir / 'Протоколы_сняты.xlsx')

    return dfs_union


def calculate_current_rank(application_config: ApplicationConfig, rank_formula_config: RankFormulaConfig,
                           protocols_df: pd.DataFrame) -> pd.DataFrame:
    def remove_duplicates_and_convert_to_str(s):
        return ''.join(set(s))

    protocols_df['Коэффициент вида старта'] = protocols_df['Соревнование'].str.findall('общий старт').apply(
        remove_duplicates_and_convert_to_str)
    protocols_df.to_excel(application_config.rank_dir / 'Протоколы.xlsx')
    return pd.DataFrame()


def save_current_rank(application_config: ApplicationConfig, current_rank_df: pd.DataFrame):
    pass


if __name__ == '__main__':
    setup_logging()
    try:
        main()
    except Exception as e:
        logging.exception(e)
