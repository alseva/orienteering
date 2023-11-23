import os
from datetime import datetime
import re
import pandas as pd
from pandas import DataFrame
from bs4 import BeautifulSoup
import logging
from logger import setup_logging
import numpy as np
import requests

from app_config import ApplicationConfig
from rank_formula_config import RankFormulaConfig
from constants import APP_CONFIG_FILE, RANK_CONFIG_FILE


def download_protocols(application_config: ApplicationConfig):
    for url in application_config.protocol_urls_df['Ссылка']:
        name = url.split('/')[-1]
        with open(application_config.protocols_dir / name, 'w') as f:
            f.write(requests.get(url).text)


def prepare_protocols(application_config: ApplicationConfig, rank_formula_config: RankFormulaConfig) -> tuple[
    DataFrame, DataFrame, DataFrame]:
    dfs_union = pd.DataFrame()
    df_not_started = pd.DataFrame()
    df_left_race = pd.DataFrame()

    logging.info('--Обработка протоколов')

    # одна итерация - один протокол  -----------------------------------------------------------------------------------
    for name in os.listdir(application_config.protocols_dir):
        if os.path.isfile(os.path.join(application_config.protocols_dir, name)):
            dfs = pd.read_html(application_config.protocols_dir / name)

            soup = BeautifulSoup(open(application_config.protocols_dir / name, 'r'), 'lxml')

            heading2 = ['h2']
            heading1 = ['h1']

            header1 = str(soup.find_all(heading1)[0].text.strip())
            competition = ''.join(re.split('\\n', header1)[0]).replace('Протокол результатов', '').strip()

            date_string = re.findall('[0-9]{2}\.[0-9]{2}\.[0-9]{4}', header1)
            if len(date_string) > 0:
                date_string = datetime.strptime(date_string[0], '%d.%m.%Y')
            elif len(re.findall('[0-9]{8}_', name)) > 0:
                date_string = datetime.strptime(re.findall('[0-9]{8}_', name)[0].replace('_', ''), '%Y%m%d')
            else:
                date_string = os.path.getctime(os.path.join(application_config.protocols_dir, name))
            competition_date = date_string.date()

            # определяем сезон протокола (для зимнего ранга декабрь относится к следующему году)
            if application_config.rank_to_calculate == 'Общий зимний ранг' and competition_date.month == 12:
                competition_year = competition_date.year + 1
            else:
                competition_year = competition_date.year

            # обрабатываем только протоколы, относящиеся к сезону, заданному в конфигураторе
            if competition_year == application_config.season:
                logging.info(name)
                # одна итерация - одна возрастная группа -------------------------------------------------------------------
                for tbl in range(len(dfs)):

                    # Трансформация колонок протокола ----------------------------------------------------------------------
                    if 'Фамилия, Имя' in dfs[tbl].columns:
                        dfs[tbl]['Фамилия'] = dfs[tbl]['Фамилия, Имя'].astype(str).str.split(' ').map(lambda x: x[0])
                        dfs[tbl]['Имя'] = dfs[tbl]['Фамилия, Имя'].astype(str).str.split(' ').map(lambda x: x[1:])
                        dfs[tbl]['Имя'] = dfs[tbl]['Имя'].str.join(' ')
                        dfs[tbl].drop(labels='Фамилия, Имя', axis=1, inplace=True)

                    if 'Г.р' in dfs[tbl].columns:
                        dfs[tbl].rename(columns={'Г.р': 'Г.р.'}, inplace=True)

                    dfs[tbl] = dfs[tbl].astype({'Фамилия': 'string', 'Имя': 'string'})
                    dfs[tbl]['Возрастная группа'] = str(soup.find_all(heading2)[tbl].text.strip()).upper()
                    dfs[tbl]['Пол'] = str(soup.find_all(heading2)[tbl].text.strip()).upper()[:1]

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
                    dfs[tbl]['Г.р.'].replace(np.nan, 0, inplace=True)

                    dfs[tbl] = dfs[tbl].merge(application_config.mapping_correct_participant_data,
                                              how='left',
                                              on=['Фамилия', 'Имя', 'Г.р.'],
                                              suffixes=(None, '_map'))
                    dfs[tbl]['Фамилия верная'].fillna(dfs[tbl]['Фамилия'], inplace=True)
                    dfs[tbl].drop(labels='Фамилия', axis=1, inplace=True)
                    dfs[tbl].rename(columns={'Фамилия верная': 'Фамилия'}, inplace=True)

                    dfs[tbl]['Имя верное'].fillna(dfs[tbl]['Имя'], inplace=True)
                    dfs[tbl].drop(labels='Имя', axis=1, inplace=True)
                    dfs[tbl].rename(columns={'Имя верное': 'Имя'}, inplace=True)

                    dfs[tbl]['Г.р. верный'].fillna(dfs[tbl]['Г.р.'], inplace=True)
                    dfs[tbl].drop(labels='Г.р.', axis=1, inplace=True)
                    dfs[tbl].rename(columns={'Г.р. верный': 'Г.р.'}, inplace=True)

                    dfs[tbl]['Результат'].replace('п\.п\. .*', 'cнят', inplace=True, regex=True)
                    dfs[tbl]['Результат'].replace('cнят (запр.)', 'cнят', inplace=True, regex=False)

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
                        if len(re.findall('.*клубн.*куб.*карели.*', s)) > 0 or len(re.findall('.*ккк.*', s)) > 0:
                            return 'Клубный кубок Карелии (ККК)'

                    dfs[tbl]['Уровень старта'] = dfs[tbl]['Уровень старта'].apply(race_level_mapping)
                    dfs[tbl] = dfs[tbl].merge(rank_formula_config.race_level_df,
                                              how='left',
                                              on='Уровень старта',
                                              suffixes=(None, '_map'))
                    dfs[tbl]['Коэффициент уровня старта'] = 1 + dfs[tbl]['Коэффициент уровня старта'].fillna(0)
                    dfs[tbl] = dfs[tbl].merge(rank_formula_config.group_rank_df,
                                              how='left',
                                              on='Возрастная группа',
                                              suffixes=(None, '_config'))

                    # оставляем для расчета ранга только группы МЖ12 и старше
                    dfs[tbl] = dfs[tbl][
                        dfs[tbl]['Возрастная группа'].isin(
                            rank_formula_config.group_rank_df['Возрастная группа'].to_list())]

                    # дополняем таблицы со снятыми и не стартовавшими
                    df_not_started = df_not_started.append(dfs[tbl][dfs[tbl]['Результат'] == 'н/с'])
                    df_left_race = df_left_race.append(dfs[tbl][dfs[tbl]['Результат'] == 'cнят'])

                    # фильтруем снятых, не стартовавших или без имени или фамилии или вместо места поставлен прочерк
                    dfs[tbl] = dfs[tbl][~((dfs[tbl]['Результат'] == 'cнят') | (dfs[tbl]['Результат'] == 'н/с') | (
                        dfs[tbl]['Фамилия'].isna()) | (dfs[tbl]['Имя'].isna()) | (dfs[tbl]['Место'] == '-'))]

                    dfs[tbl]['Место'] = dfs[tbl]['Место'].astype(int)

                    dfs[tbl]['Результат'] = pd.to_datetime(dfs[tbl]['Результат'])
                    dfs[tbl]['result_in_seconds'] = (
                            dfs[tbl]['Результат'].dt.hour * 60 * 60 +
                            dfs[tbl]['Результат'].dt.minute * 60 +
                            dfs[tbl]['Результат'].dt.second)
                    dfs[tbl]['Результат'] = dfs[tbl]['Результат'].dt.time
                    # ------------------------------------------------------------------------------------------------------

                # дополнем таблицу протоколов обработанным экземпляром
                dfs_union = dfs_union.append(pd.concat(dfs))
                dfs_union.reset_index(inplace=True, drop=True)

                df_not_started.to_excel(
                    application_config.rank_dir / 'Протоколы_не_стартовали_{}.xlsx'.format(competition_year),
                    index=False)
                df_left_race.to_excel(application_config.rank_dir / 'Протоколы_сняты_{}.xlsx'.format(competition_year),
                                      index=False)
                dfs_union[dfs_union['Г.р.'] == 0][[
                    'Дата соревнования', 'Соревнование', 'Фамилия', 'Имя', 'Г.р.', 'Возрастная группа']].to_excel(
                    application_config.rank_dir / 'Участники без года рождения_{}.xlsx'.format(competition_year),
                    index=False)
    return dfs_union, df_left_race, df_not_started


def download_prepare_protocols():
    application_config = ApplicationConfig(APP_CONFIG_FILE)
    rank_formula_config = RankFormulaConfig(RANK_CONFIG_FILE)

    if application_config.protocol_source_type == 'Ссылка':
        download_protocols(application_config)

    prepare_protocols(application_config, rank_formula_config)


if __name__ == '__main__':
    setup_logging()
    download_prepare_protocols()
