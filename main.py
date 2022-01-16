import logging
import os
import re
import warnings

import pandas as pd
from bs4 import BeautifulSoup

from app_config import ApplicationConfig
from constants import APP_CONFIG_FILE, RANK_CONFIG_FILE
from errors import Error, AppConfigValidationError, RankConfigValidationError
from logger import setup_logging
from rank_formula_config import RankFormulaConfig
from validation.app_config_validation import check_app_config
from validation.rank_config_validation import check_rank_config


def main():
    application_config = ApplicationConfig(APP_CONFIG_FILE)
    rank_formula_config = RankFormulaConfig(RANK_CONFIG_FILE)
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
                dfs[tbl]['Уровень старта'] = header1
                dfs[tbl]['Файл протокола'] = name
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
        s = ''.join(set(s))
        return s if len(s) > 0 else 'раздельный старт'

    protocols_df['Вид старта'] = protocols_df['Соревнование'].str.findall('общий старт').apply(
        remove_duplicates_and_convert_to_str)
    protocols_df = protocols_df.merge(rank_formula_config.race_type_df,
                                      how='left',
                                      on='Вид старта',
                                      suffixes=(None, '_map'))

    def race_level_mapping(s):
        s = s.lower()
        if len(re.findall('.*((чемпионат)|(первенство))+.*петрозаводск.*', s)) > 0:
            return 'Чемпионат и первенство г.Петрозаводска'
        if len(re.findall('.*((чемпионат)|(первенство))+.*карелия.*', s)) > 0:
            return 'Чемпионат и первенство Республики Карелия'
        if len(re.findall('.*онежск.*весн.*', s)) > 0:
            return 'Онежская весна'
        if len(re.findall('.*всероссийские.*соревнования.*', s)) > 0:
            return 'Всероссийские соревнования'
        if len(re.findall('.*клубн.*куб.*карели.*', s)) > 0:
            return 'Клубный кубок Карелии'

    protocols_df['Уровень старта'] = protocols_df['Уровень старта'].apply(race_level_mapping)
    protocols_df = protocols_df.merge(rank_formula_config.race_level_df,
                                      how='left',
                                      on='Уровень старта',
                                      suffixes=(None, '_map'))
    # protocols_df_grouped = protocols_df.groupby(by=['Файл протокола', 'Группа'])
    protocols_df.to_excel(application_config.rank_dir / 'Протоколы.xlsx')

    return pd.DataFrame()


def save_current_rank(application_config: ApplicationConfig, current_rank_df: pd.DataFrame):
    pass


if __name__ == '__main__':
    setup_logging()
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            check_app_config()
            check_rank_config()
            main()
    except AppConfigValidationError as e:
        logging.error(f'Application config validation failed. {e}')
    except RankConfigValidationError as e:
        logging.error(f'Rank config validation failed. {e}')
    except Error as e:
        logging.error(e)
    except Exception as e:
        logging.exception(e)
