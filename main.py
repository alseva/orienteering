import logging
import os
import re
import warnings
from datetime import datetime

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

from app_config import ApplicationConfig
from constants import APP_CONFIG_FILE, RANK_CONFIG_FILE, VERSION
from errors import Error
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

                date_string = re.findall('[0-9]{2}\.[0-9]{2}\.[0-9]{4}', header1)
                if len(date_string) > 0:
                    date_string = datetime.strptime(date_string[0], '%d.%m.%Y')
                elif len(re.findall('[0-9]{8}_', name)) > 0:
                    date_string = datetime.strptime(re.findall('[0-9]{8}_', name)[0].replace('_', ''), '%Y%m%d')
                else:
                    date_string = os.path.getctime(os.path.join(application_config.protocols_dir, name))
                competition_date = date_string.date()

                # Transform protocol columns ---------------------------------------------------------------------------
                dfs[tbl] = dfs[tbl].astype({'Фамилия': 'string', 'Имя': 'string'})
                dfs[tbl]['Возрастная группа'] = str(soup.find_all(heading2)[tbl].text.strip()).upper()

                dfs[tbl] = dfs[tbl].merge(application_config.mapping_group_df,
                                          how='left',
                                          on='Возрастная группа',
                                          suffixes=(None, '_config'))
                dfs[tbl]['Возрастная группа верная'].fillna(dfs[tbl]['Возрастная группа'], inplace=True)
                dfs[tbl].drop(labels='Возрастная группа', axis=1, inplace=True)
                dfs[tbl].rename(columns={'Возрастная группа верная': 'Возрастная группа'}, inplace=True)

                dfs[tbl]['Соревнование'] = competition
                dfs[tbl]['Дата соревнования'] = competition_date
                dfs[tbl]['Уровень старта'] = header1
                dfs[tbl]['Файл протокола'] = name
                dfs[tbl]['Фамилия'] = dfs[tbl]['Фамилия'].str.upper()
                dfs[tbl]['Имя'] = dfs[tbl]['Имя'].str.upper()

                dfs[tbl] = dfs[tbl].merge(application_config.mapping_yob_df,
                                          how='left',
                                          on=['Фамилия', 'Имя'],
                                          suffixes=(None, '_map'))
                dfs[tbl]['Г.р.'].replace(0, np.nan, inplace=True)
                dfs[tbl]['Г.р.'] = dfs[tbl]['Г.р.'].fillna(dfs[tbl]['Г.р._map'])
                dfs[tbl].drop(labels='Г.р._map', axis=1, inplace=True)

                def remove_duplicates_and_convert_to_str(s):
                    s = ''.join(set(s))
                    return s if len(s) > 0 else 'раздельный старт'

                dfs[tbl]['Вид старта'] = dfs[tbl]['Соревнование'].str.findall('общий старт').apply(
                    remove_duplicates_and_convert_to_str)
                dfs[tbl] = dfs[tbl].merge(rank_formula_config.race_type_df,
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

                dfs[tbl]['Уровень старта'] = dfs[tbl]['Уровень старта'].apply(race_level_mapping)
                dfs[tbl] = dfs[tbl].merge(rank_formula_config.race_level_df,
                                          how='left',
                                          on='Уровень старта',
                                          suffixes=(None, '_map'))
                dfs[tbl] = dfs[tbl].merge(rank_formula_config.group_rank_df,
                                          how='left',
                                          on='Возрастная группа',
                                          suffixes=(None, '_config'))
                # Transform protocol columns ---------------------------------------------------------------------------

                df_not_started = df_not_started.append(dfs[tbl][dfs[tbl]['Результат'] == 'н/с'])
                df_left_race = df_left_race.append(dfs[tbl][dfs[tbl]['Результат'] == 'cнят'])

                # filter records without result or without last/first name
                dfs[tbl] = dfs[tbl][~((dfs[tbl]['Результат'] == 'cнят') | (dfs[tbl]['Результат'] == 'н/с') | (
                    dfs[tbl]['Фамилия'].isna()) | (dfs[tbl]['Имя'].isna()))]
                # filter records without result

                # filter open and other not relevant groups
                dfs[tbl] = dfs[tbl][
                    dfs[tbl]['Возрастная группа'].isin(
                        rank_formula_config.group_rank_df['Возрастная группа'].to_list())]
                # filter open and other not relevant groups

                dfs[tbl]['Результат'] = pd.to_datetime(dfs[tbl]['Результат'])
                dfs[tbl]['result_in_seconds'] = (
                        dfs[tbl]['Результат'].dt.hour * 60 * 60 +
                        dfs[tbl]['Результат'].dt.minute * 60 +
                        dfs[tbl]['Результат'].dt.second)
                dfs[tbl]['Результат'] = dfs[tbl]['Результат'].dt.time

            dfs_union = dfs_union.append(pd.concat(dfs))
            dfs_union.reset_index(inplace=True, drop=True)
            dfs_union.to_excel(application_config.rank_dir / 'Протоколы.xlsx')
            df_not_started.to_excel(application_config.rank_dir / 'Протоколы_не_стартовали.xlsx')
            df_left_race.to_excel(application_config.rank_dir / 'Протоколы_сняты.xlsx')

    return dfs_union


def calculate_current_rank(application_config: ApplicationConfig, rank_formula_config: RankFormulaConfig,
                           protocols_df: pd.DataFrame) -> pd.DataFrame:
    protocols_rank_df = pd.DataFrame.from_dict({'Кол-во прошедших соревнований': []})
    current_rank_df = pd.DataFrame()
    protocols_rank_df_final = pd.DataFrame.from_dict({'Кол-во прошедших соревнований': []})
    participant_fields = ['Фамилия', 'Имя', 'Г.р.']
    for competition in protocols_df.sort_values(by='Дата соревнования')['Файл протокола'].unique():
        print(competition)
        protocol_df = protocols_df[protocols_df['Файл протокола'] == competition].copy()

        def calculate_competition_rank(df):
            participants_number = len(df)
            if participants_number >= 7:
                top_result = 4
                top_relative_rank_results = 6
            elif participants_number == 6:
                top_result = 3
                top_relative_rank_results = 5
            elif participants_number == 5:
                top_result = 3
                top_relative_rank_results = participants_number
            else:
                top_result = 2
                top_relative_rank_results = participants_number

            def get_mean_by_top(df, top, asc):
                return df.sort_values(axis=0, ascending=asc).head(top).mean()

            df['tсравнит '] = get_mean_by_top(df['result_in_seconds'], top_result, asc=True)
            if len(current_rank_df) > 0:
                df_top = df.sort_values(by='result_in_seconds', axis=0, ascending=True).head(
                    top_relative_rank_results).merge(current_rank_df,
                                                     how='left',
                                                     on=participant_fields,
                                                     suffixes=(None, '_config'))
                df['Сравнит. ранг соревнований'] = get_mean_by_top(df_top['Текущий ранг'], top_relative_rank_results,
                                                                   asc=False)
            else:
                df['Сравнит. ранг соревнований'] = df['Ранг группы']
            df['N'] = participants_number if participants_number > 1 else 2
            df['Ранг по группе'] = (df['tсравнит '] / df['result_in_seconds']) * df['Сравнит. ранг соревнований'] * (
                    1 - df['Коэффициент вида старта'] * (df['Место'] - 1) / (df['N'] - 1))
            return df

        protocol_df = protocol_df.groupby(by=['Файл протокола', 'Возрастная группа'], as_index=False).apply(
            calculate_competition_rank)

        protocol_df['Ранг'] = protocol_df.groupby(by=['Файл протокола'] + participant_fields, as_index=False)[
            'Ранг по группе'].transform(lambda x: x.max())
        protocols_rank_df = protocols_rank_df.append(protocol_df)
        protocols_rank_df['Кол-во прошедших соревнований'] = np.where(
            protocols_rank_df['Файл протокола'] == competition,
            protocols_rank_df['Файл протокола'].nunique(), protocols_rank_df['Кол-во прошедших соревнований'])

        current_rank_df = protocols_rank_df[participant_fields + ['Ранг']].drop_duplicates()
        current_rank_df.rename(columns={'Ранг': 'Текущий ранг'}, inplace=True)
        current_rank_df = current_rank_df.groupby(by=participant_fields, as_index=False).agg(
            {'Текущий ранг': np.mean})
        current_rank_df.sort_values(by='Текущий ранг', ascending=False, inplace=True)
        current_rank_df.reset_index(drop=True, inplace=True)
        protocol_df = protocol_df.merge(current_rank_df,
                                        how='left',
                                        on=participant_fields,
                                        suffixes=(None, '_config'))

        protocols_rank_df_final = protocols_rank_df_final.append(protocol_df)
        protocols_rank_df_final['Кол-во прошедших соревнований'] = np.where(
            protocols_rank_df_final['Файл протокола'] == competition,
            protocols_rank_df_final['Файл протокола'].nunique(), protocols_rank_df_final['Кол-во прошедших соревнований'])
        protocols_rank_df_final.sort_values(by=['Дата соревнования', 'Возрастная группа', 'Место'], inplace=True)
    protocols_rank_df_final.to_excel(application_config.rank_dir / 'Протоколы.xlsx')
    current_rank_df.to_excel(application_config.rank_dir / 'Текущий ранг.xlsx')
    return pd.DataFrame()


def save_current_rank(application_config: ApplicationConfig, current_rank_df: pd.DataFrame):
    pass


if __name__ == '__main__':
    setup_logging()
    logging.info(f'Rank calculator version {VERSION}\nAlex & Oleg, Inc. No rights are reserved.\n')
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            check_app_config()
            check_rank_config()
            main()
    except Error as e:
        logging.error(e)
    except Exception as e:
        logging.exception(e)
