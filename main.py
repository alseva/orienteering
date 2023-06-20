import logging
import math
import os
import re
import urllib
import urllib.request
import warnings
from datetime import datetime
from decimal import *

getcontext().prec = 28

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from pandas import DataFrame

from app_config import ApplicationConfig
from constants import APP_CONFIG_FILE, RANK_CONFIG_FILE, VERSION
from errors import Error
from logger import setup_logging
from rank_formula_config import RankFormulaConfig
from validation.app_config_validation import check_app_config
from validation.rank_config_validation import check_rank_config


def get_decimal(s):
    return Decimal(s)


def main():
    application_config = ApplicationConfig(APP_CONFIG_FILE)
    rank_formula_config = RankFormulaConfig(RANK_CONFIG_FILE)

    if application_config.rank_to_calculate == 'Лесной ранг':
        race_number_to_start_apply_rules = rank_formula_config.race_number_to_start_apply_rules_forest_rank
        race_number_to_start_apply_relative_rank = rank_formula_config.race_number_to_start_apply_relative_rank_forest
    elif application_config.rank_to_calculate == 'Спринт ранг':
        race_number_to_start_apply_rules = rank_formula_config.race_number_to_start_apply_rules_sprint_rank
        race_number_to_start_apply_relative_rank = rank_formula_config.race_number_to_start_apply_relative_rank_sprint
    else:
        race_number_to_start_apply_rules = rank_formula_config.race_number_to_start_apply_rules
        race_number_to_start_apply_relative_rank = rank_formula_config.race_number_to_start_apply_relative_rank

    logging.info('Сезон: ' + str(application_config.season))
    logging.info(application_config.rank_to_calculate)

    if application_config.protocol_source_type == 'Ссылка':
        download_protocols(application_config)
    df_previous_year_final_rank = get_previous_year_final_rank(application_config)
    protocols_df, left_races_df, df_not_started = prepare_protocols(application_config, rank_formula_config)
    current_rank_df = calculate_current_rank(application_config, rank_formula_config, protocols_df, left_races_df,
                                             df_previous_year_final_rank,
                                             race_number_to_start_apply_rules, race_number_to_start_apply_relative_rank)
    save_current_rank(application_config, current_rank_df)
    transform_and_save_not_started_and_left_race(application_config, left_races_df, df_not_started)


def download_protocols(application_config: ApplicationConfig):
    for url in application_config.protocol_urls_df['Ссылка']:
        name = url.split('/')[-1]
        result = urllib.request.urlopen(url)
        html_content = ''.join(line.decode('utf-8') for line in result)
        with open(application_config.protocols_dir / name, 'w') as f:
            f.write(html_content)


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


def get_previous_year_final_rank(application_config: ApplicationConfig):
    df_previous_year_final_rank = pd.DataFrame.from_dict(
        {'Фамилия': [], 'Имя': [], 'Г.р.': [], 'Пол': [], 'Ранг': [], 'Флаг финального ранга прошлого сезона': []})
    for name in os.listdir(application_config.previous_year_final_rank_file):
        previous_year_final_rank_file = os.path.join(application_config.previous_year_final_rank_file, name)
        if os.path.isfile(previous_year_final_rank_file) and (
                int(name.replace('.xlsx', '').split('_')[-1]) == application_config.season - 1):
            participant_fields = ['Фамилия', 'Имя', 'Г.р.', 'Пол']
            df_previous_year_final_rank = pd.read_excel(previous_year_final_rank_file)
            for col in df_previous_year_final_rank.columns:
                if re.match('Итоговый ранг сезона [0-9]{4}', col):
                    previous_year_final_rank_column = col
            df_previous_year_final_rank = df_previous_year_final_rank[
                df_previous_year_final_rank[previous_year_final_rank_column] > 0]
            df_previous_year_final_rank['Фамилия'] = df_previous_year_final_rank['Участник'].astype(str).str.split(
                ' ').map(
                lambda x: x[0])
            df_previous_year_final_rank['Имя'] = df_previous_year_final_rank['Участник'].astype(str).str.split(' ').map(
                lambda x: x[1:])
            df_previous_year_final_rank['Имя'] = df_previous_year_final_rank['Имя'].str.join(' ')
            df_previous_year_final_rank.rename(columns={previous_year_final_rank_column: 'Ранг'}, inplace=True)
            df_previous_year_final_rank = df_previous_year_final_rank.astype(
                {'Фамилия': 'string', 'Имя': 'string', 'Пол': 'string', 'Г.р.': 'int'})
            df_previous_year_final_rank = df_previous_year_final_rank[participant_fields + ['Ранг']]
            df_previous_year_final_rank = df_previous_year_final_rank[df_previous_year_final_rank['Ранг'] > 0]
            df_previous_year_final_rank['Флаг финального ранга прошлого сезона'] = True
            df_previous_year_final_rank['Ранг'] = df_previous_year_final_rank['Ранг'].apply(get_decimal)
            logging.info('--Прошлогодний ранг (сезон {}) идет в учет'.format(application_config.season - 1))
    return df_previous_year_final_rank


def calculate_current_rank(application_config: ApplicationConfig, rank_formula_config: RankFormulaConfig,
                           protocols_df: pd.DataFrame, left_races_df: pd.DataFrame,
                           df_previous_year_final_rank: pd.DataFrame,
                           race_number_to_start_apply_rules, race_number_to_start_apply_relative_rank) -> pd.DataFrame:
    protocols_rank_df = pd.DataFrame.from_dict(
        {'Кол-во прошедших соревнований': [], 'Участники сравнит. ранга соревнований': []})
    current_rank_df = pd.DataFrame.from_dict(
        {'Фамилия': [], 'Имя': [], 'Г.р.': [], 'Пол': [], 'Текущий ранг': [], 'Итоговый ранг': []})
    protocols_rank_df_final = pd.DataFrame.from_dict(
        {'Кол-во прошедших соревнований': [], 'Участники сравнит. ранга соревнований': []})
    participant_fields = ['Фамилия', 'Имя', 'Г.р.', 'Пол']

    logging.info('--Расчет ранга')

    # ------------------------------------------------------------------------------------------------------------------
    # В цикле по каждому соревнованию рассчитываем ранг
    # дополняем общий файл рангов соревнований новым расчетным значением
    # рассчитываем текущий ранг - как он изменился после соревнования
    competitions_cnt = 1
    competitions_total = protocols_df.sort_values(by='Дата соревнования')['Файл протокола'].nunique()
    for competition in protocols_df.sort_values(by='Дата соревнования')['Файл протокола'].unique():
        logging.info(str(competitions_cnt) + '. ' + competition)
        protocol_df = protocols_df[protocols_df['Файл протокола'] == competition].copy()

        # дополняем протоколы снятыми учасниками (они нужны для расчета сравнительного ранга соревнований)
        left_race_df = left_races_df[left_races_df['Файл протокола'] == competition].copy()
        left_race_df['left_race'] = 'yes'
        left_race_df['Г.р.'] = left_race_df['Г.р.'].fillna(0)
        left_race_df = left_race_df[
            ['Дата соревнования', 'Соревнование', 'Файл протокола', 'Уровень старта', 'Коэффициент уровня старта',
             'Вид старта', 'Коэффициент вида старта', 'Возрастная группа', '№ п/п', 'Номер', 'Фамилия', 'Имя', 'Г.р.',
             'Пол', 'Разр.', 'Команда', 'Ранг группы', 'left_race']]
        protocol_df = pd.concat([protocol_df, left_race_df])

        # расчет ранга соревнований отдельно для каждой возрастной группы
        def calculate_competition_rank(df, competitions_cnt):
            participants_number = len(
                df[df['left_race'].isna()])  # исключаем снятных из кол-ва учасников для расчета ранга соревнований
            participants_number_for_relative_rank = len(
                df)  # учитываем снятых для расчета сравнительго ранга соревнований

            if participants_number > 8:
                top_result = 5
            elif participants_number in (7, 8):
                top_result = 4
            elif participants_number in (5, 6):
                top_result = 3
            else:
                top_result = 2

            if participants_number_for_relative_rank > 8:
                top_relative_rank_results = 7
            elif participants_number in (7, 8):
                top_relative_rank_results = 6
            elif participants_number == 6:
                top_relative_rank_results = 5
            else:
                top_relative_rank_results = participants_number_for_relative_rank

            def get_mean_by_top(s, top, asc):
                if len(s) == 1:
                    return s.iloc[0]

                s = s.sort_values(axis=0, ascending=asc)
                winner = s.iloc[0]

                while top > 1:
                    mean = s.head(top).apply(get_decimal).mean()
                    if mean / winner <= 1.15:
                        return mean
                    top -= 1

                return Decimal(winner) * Decimal(1.15)

            df['tсравнит '] = get_mean_by_top(df['result_in_seconds'], top_result, asc=True)

            # если расчет ранга осуществляется для первого соревнования в сезоне,
            # то берем итоговый ранг прошлого сезона в качестве текущего ранга для расчета сравнительного в 1ом соревновании
            if competitions_cnt == 1 and len(df_previous_year_final_rank) > 0:
                df_top = df.merge(df_previous_year_final_rank,
                                  how='left',
                                  on=participant_fields,
                                  suffixes=(None, '_config'))
                df_top.rename(columns={'Ранг': 'Текущий ранг'}, inplace=True)
            else:
                df_top = df.merge(current_rank_df,
                                  how='left',
                                  on=participant_fields,
                                  suffixes=(None, '_config'))
            # есть текущий ранг и в группе больше одного участника и номер соревнования уже позволяет использовать текущий ранг спортсменов
            if df_top['Текущий ранг'].count() > 0 and df[
                '№ п/п'].dropna().nunique() > 1 and competitions_cnt > race_number_to_start_apply_relative_rank:
                df_top.dropna(subset=['Текущий ранг'], inplace=True)
                df_top = df_top.sort_values(by='Текущий ранг', axis=0, ascending=False).head(top_relative_rank_results)
                df_top['Участники сравнит. ранга соревнований'] = (df_top['Фамилия'] + ' ' + df_top['Имя'] + ': ' +
                                                                   df_top['Текущий ранг'].apply(
                                                                       lambda x: round(x, 2)).astype(str))

                df['Сравнит. ранг соревнований'] = df_top['Текущий ранг'].apply(get_decimal).mean()
                df['Участники сравнит. ранга соревнований'] = df_top['Участники сравнит. ранга соревнований'].str.cat(
                    sep=', ')

            # текущий ранг еще отсутствует или в группе один участник
            else:
                df['Сравнит. ранг соревнований'] = df['Ранг группы']

            df['N'] = participants_number if participants_number > 1 else 2

            df['Ранг по группе'] = df['Коэффициент уровня старта'].apply(get_decimal) * (
                    df['tсравнит '].apply(get_decimal) / df['result_in_seconds'].apply(get_decimal)) * df[
                                       'Сравнит. ранг соревнований'].apply(get_decimal) * (
                                           Decimal(1) - df['Коэффициент вида старта'].apply(get_decimal) * (
                                           df['Место'].apply(get_decimal) - 1) / (df['N'].apply(get_decimal) - 1))

            return df

        protocol_df = protocol_df.groupby(by=['Файл протокола', 'Возрастная группа'], as_index=False).apply(
            calculate_competition_rank, competitions_cnt)

        # если участник в протоколе был сразу в нескольких возрастных группах,
        # то для него берется лучший ранг из рассчитанных
        protocol_df['Ранг'] = protocol_df.groupby(by=['Файл протокола'] + participant_fields, as_index=False)[
            'Ранг по группе'].transform(lambda x: x.max())

        # дополняем таблицу протоколов соревнованием с рассчитанным рангом
        protocols_rank_df = protocols_rank_df.append(protocol_df)

        # для расчета текущего ранга берем таблицу с протоколами и рангами по каждому соревнованию
        # дубликаты убираем, так как у участника может быть несколько рангов по разным возрастным группам,
        # но ранг соревнования одинаковый для них проставлен (максимальный)
        current_rank_df = protocols_rank_df[
            participant_fields + ['Файл протокола', 'Ранг', 'Дата соревнования']].drop_duplicates()

        # если кол-во стартов еще не превысило, указанное в конфигураторе для начала применения
        # правила штрафов и 50% лучших соревнований, то для расчета текущего ранга берутся все старты
        if competitions_cnt <= race_number_to_start_apply_rules:
            current_rank_df['Кол-во cоревнований для текущего ранга'] = competitions_cnt
        else:
            current_rank_df['Кол-во cоревнований для текущего ранга'] = math.ceil(
                competitions_cnt * rank_formula_config.race_percentage_for_final_rank)

        current_rank_df['Кол-во прошедших соревнований'] = competitions_cnt

        # кол-во соревнований у каждого участника
        races_per_participant_df = current_rank_df.groupby(by=participant_fields, as_index=False).agg(
            {'Файл протокола': 'nunique'})
        races_per_participant_df.rename(columns={'Файл протокола': 'Кол-во соревнований у участника'}, inplace=True)
        current_rank_df = current_rank_df.merge(races_per_participant_df, how='left', on=participant_fields)

        # если кол-во стартов еще не превысило, указанное в конфигураторе для начала применения
        # правила штрафов и 50% лучших соревнований, то долю/кол-во отсутствующих стартов считают = 0
        if competitions_cnt <= race_number_to_start_apply_rules:
            current_rank_df['Доля отсутствующих стартов'] = 0
            current_rank_df['% интервал отсутствующих стартов'] = 0
        else:
            current_rank_df['Доля отсутствующих стартов'] = (1 -
                                                             current_rank_df['Кол-во соревнований у участника'].apply(
                                                                 get_decimal) /
                                                             current_rank_df[
                                                                 'Кол-во cоревнований для текущего ранга'].apply(
                                                                 get_decimal))
            current_rank_df['Доля отсутствующих стартов'] = current_rank_df['Доля отсутствующих стартов'].apply(
                lambda x: math.floor(x * 100) / 100 if x > 0 else 0)
            current_rank_df['% интервал отсутствующих стартов'] = (
                    (1 -
                     current_rank_df['Кол-во соревнований у участника'].apply(get_decimal) /
                     current_rank_df['Кол-во cоревнований для текущего ранга'].apply(get_decimal))
                    * 100).apply(lambda x: math.floor(x))

        def define_lack_races(x):
            if x >= 80.0:
                return '80% и более'
            elif x >= 60.0:
                return '60% - 79%'
            elif x >= 40.0:
                return '40% - 59%'
            elif x >= 20.0:
                return '20% - 39%'
            else:
                return '-'

        current_rank_df['% интервал отсутствующих стартов'] = current_rank_df[
            '% интервал отсутствующих стартов'].apply(define_lack_races)

        current_rank_df = current_rank_df.merge(rank_formula_config.penalty_lack_races_df,
                                                how='left',
                                                on='% интервал отсутствующих стартов')

        current_rank_df['Штраф за отсутствующие старты'] = Decimal(1) - current_rank_df['Штраф за отсутствующие старты'] \
            .fillna(Decimal(0)) \
            .apply(get_decimal)

        current_rank_fields = ['Кол-во cоревнований для текущего ранга',
                               'Кол-во прошедших соревнований',
                               'Кол-во соревнований у участника',
                               'Доля отсутствующих стартов',
                               '% интервал отсутствующих стартов',
                               'Штраф за отсутствующие старты']

        # расчет топ рангов соревнований
        def define_top_races_for_current_rank(df):
            races_number = df['Кол-во cоревнований для текущего ранга'].max()
            df = df.sort_values(by='Ранг', ascending=False).head(races_number)
            return df

        # расчет текущего ранга: среднее лучших рангов соревнований (с учетом прошлогоднего итогового ранга),
        # скорректированное на штраф за отсутствующие старты
        def define_current_rank(df):
            penalty_lack_races = df['Штраф за отсутствующие старты'].max()
            df['Текущий ранг'] = Decimal(df['Ранг'].mean()) * Decimal(penalty_lack_races)
            return df

        # получаем топ рангов соревнований для каждого участника
        current_rank_df = current_rank_df.groupby(by=participant_fields, as_index=False).apply(
            define_top_races_for_current_rank)

        # добавляем финальный ранг прошлого сезона, если он есть, и рассчитываем текущий ранг для каждого участника
        if len(df_previous_year_final_rank) > 0:
            current_rank_df = current_rank_df.append(df_previous_year_final_rank)
        current_rank_df = current_rank_df.groupby(by=participant_fields, as_index=False).apply(define_current_rank)

        # если расчет по последнему соревнованию в сезоне, то применяем правила обнуления
        if application_config.last_race_flag == 'да' and competitions_cnt == competitions_total:
            current_rank_df['Итоговый ранг'] = np.where(
                current_rank_df[
                    'Доля отсутствующих стартов'] >= rank_formula_config.race_percentage_to_reset_final_rank,
                0,
                current_rank_df['Текущий ранг'])
        else:
            current_rank_df['Итоговый ранг'] = np.nan

        # добавляем дату текущего соревнования
        current_rank_df['Дата текущего соревнования'] = current_rank_df[current_rank_df['Дата соревнования'].notna()][
            'Дата соревнования'].max()

        # оставляем только строку с последним текущим рангом, доступным у каждого участника
        def left_current_rank_only(df):
            df = df.sort_values(by='Дата соревнования', ascending=False).head(1)
            return df

        current_rank_df = current_rank_df.groupby(by=participant_fields, as_index=False).apply(left_current_rank_only)

        # добавляем текущий ранг к протоколу текущего соревнования
        protocol_df = protocol_df.merge(current_rank_df,
                                        how='left',
                                        on=participant_fields + ['Файл протокола'],
                                        suffixes=(None, '_config'))

        # сохраняем текущий ранг в файл
        current_rank_df = current_rank_df[participant_fields + ['Текущий ранг', 'Итоговый ранг'] +
                                          current_rank_fields + ['Дата текущего соревнования']]
        current_rank_df.sort_values(by='Текущий ранг', ascending=False, inplace=True)
        current_rank_df.reset_index(drop=True, inplace=True)
        current_rank_df.index += 1
        current_rank_df.to_excel(
            application_config.rank_dir / 'Текущий ранг_{}_{}.xlsx'.format(competition, application_config.season))

        # добавляем протокол соревнования к общей таблице протоколов
        protocols_rank_df_final = protocols_rank_df_final.append(protocol_df)

        # считаем кол-во прошедших соревнований только для текущего
        # (для предыдущих соревнований это число уже посчитано в прошлых итерациях)
        protocols_rank_df_final['Кол-во прошедших соревнований'] = np.where(
            protocols_rank_df_final['Файл протокола'] == competition,
            protocols_rank_df_final['Файл протокола'].nunique(),
            protocols_rank_df_final['Кол-во прошедших соревнований'])

        protocols_rank_df_final.sort_values(by=['Дата соревнования', 'Возрастная группа', 'Место'], inplace=True)
        competitions_cnt += 1
    # ------------------------------------------------------------------------------------------------------------------

    protocols_rank_df_final = protocols_rank_df_final[
        ['Дата соревнования', 'Соревнование', 'Файл протокола', 'Уровень старта', 'Коэффициент уровня старта',
         'Вид старта', 'Коэффициент вида старта', 'Возрастная группа', '№ п/п', 'Номер', 'Фамилия', 'Имя',
         'Г.р.', 'Пол', 'Разр.', 'Команда', 'Результат', 'Место', 'Отставание', 'Ранг группы', 'result_in_seconds',
         'tсравнит ', 'Сравнит. ранг соревнований', 'Участники сравнит. ранга соревнований', 'N', 'Ранг по группе',
         'Ранг', 'Кол-во соревнований у участника', 'Доля отсутствующих стартов', 'Штраф за отсутствующие старты',
         'Текущий ранг', 'Дата текущего соревнования', 'Итоговый ранг', 'Кол-во прошедших соревнований',
         'Кол-во cоревнований для текущего ранга']]
    protocols_rank_df_final.to_excel(
        application_config.rank_dir / 'Протоколы {}_{}.xlsx'.format(application_config.rank_to_calculate,
                                                                    application_config.season), index=False)

    return current_rank_df


def save_current_rank(application_config: ApplicationConfig, current_rank_df: pd.DataFrame):
    final_rank_date = current_rank_df['Дата текущего соревнования'].max()
    final_rank_year = current_rank_df['Дата текущего соревнования'].max().year
    current_rank_df.dropna(subset=['Текущий ранг'], inplace=True)
    current_rank_df.index.name = '№ М+Ж'
    current_rank_df['Участник'] = current_rank_df['Фамилия'] + ' ' + current_rank_df['Имя']
    current_rank_df['Г.р.'] = current_rank_df['Г.р.'].astype(int)
    current_rank_df['Текущий ранг'] = current_rank_df['Текущий ранг'].apply(lambda x: round(x, 2))
    rank_name = '{} на '.format(application_config.rank_to_calculate) + str(final_rank_date)
    columns = {'Текущий ранг': rank_name,
               'Кол-во прошедших соревнований': '№ Старта',
               'Кол-во cоревнований для текущего ранга': 'В учет',
               'Кол-во соревнований у участника': 'У участника',
               'Доля отсутствующих стартов': '% пропусков',
               '% интервал отсутствующих стартов': '# пропусков',
               'Штраф за отсутствующие старты': 'Штраф'}
    fields_to_save = ['Участник', 'Г.р.', 'Пол', rank_name, '№ Старта', 'В учет', 'У участника', 'Штраф']
    fields_to_highlight = [rank_name]
    if application_config.last_race_flag == 'да':
        current_rank_df['Итоговый ранг'] = current_rank_df['Итоговый ранг'].apply(lambda x: round(x, 2))
        final_rank_name = 'Итоговый ранг сезона {}'.format(final_rank_year)
        columns['Итоговый ранг'] = final_rank_name
        fields_to_save.append(final_rank_name)
        fields_to_highlight.append(final_rank_name)

    current_rank_df.rename(columns=columns, inplace=True)
    current_rank_df['Штраф'] = ((Decimal(1) - current_rank_df['Штраф']) * 100).apply(lambda x: round(x)).astype(
        int).astype(
        str) + '%'
    current_rank_df['Штраф'] = current_rank_df['Штраф'].apply(lambda x: x if x != '0%' else '-')
    current_rank_df = current_rank_df[fields_to_save]

    # current_rank_df = current_rank_df.style.background_gradient(cmap=application_config.rank_color, subset=rank_name)

    def highlight_col(x):
        df = x.copy()
        mask = df['Пол'] == 'Ж'
        df.loc[mask, :] = 'background-color: pink'
        df.loc[~mask, :] = 'background-color: lightblue'
        return df

    current_rank_df_mix = current_rank_df.copy()
    current_rank_df_mix = current_rank_df_mix.style.apply(highlight_col, axis=None,
                                                          subset=['Участник', 'Г.р.', 'Пол', rank_name])
    current_rank_df_mix.to_excel(
        application_config.rank_dir / (rank_name + " цветной_{}.xlsx".format(application_config.season)))

    current_rank_df_male = current_rank_df[current_rank_df['Пол'] == 'М'].copy()
    current_rank_df_male.reset_index(drop=False, inplace=True)
    current_rank_df_male.index += 1
    current_rank_df_male.index.name = '№'
    current_rank_df_male.drop(labels='Пол', axis=1, inplace=True)
    current_rank_df_male = current_rank_df_male.style.background_gradient(cmap=application_config.rank_color,
                                                                          subset=fields_to_highlight)
    current_rank_df_male.to_excel(
        application_config.rank_dir / (rank_name + " мужчины_{}.xlsx".format(application_config.season)))

    current_rank_df_female = current_rank_df[current_rank_df['Пол'] == 'Ж'].copy()
    current_rank_df_female.reset_index(drop=False, inplace=True)
    current_rank_df_female.index += 1
    current_rank_df_female.index.name = '№'
    current_rank_df_female.drop(labels='Пол', axis=1, inplace=True)
    current_rank_df_female = current_rank_df_female.style.background_gradient(cmap=application_config.rank_color,
                                                                              subset=fields_to_highlight)

    current_rank_df_female.to_excel(
        application_config.rank_dir / (rank_name + " женщины_{}.xlsx".format(application_config.season)))

    current_rank_df = current_rank_df.style.background_gradient(cmap=application_config.rank_color,
                                                                subset=fields_to_highlight)
    current_rank_df.to_excel(application_config.rank_dir / (rank_name + "_{}.xlsx".format(application_config.season)))

    pass


def transform_and_save_not_started_and_left_race(application_config, left_races_df, df_not_started):
    logging.info('--Не стартовавшие')
    df_not_started = df_not_started[['Фамилия', 'Имя', 'Г.р.', 'Файл протокола']]
    df_not_started = df_not_started.dropna()
    df_not_started = df_not_started.groupby(by=['Фамилия', 'Имя', 'Г.р.'], as_index=False).count()
    df_not_started.rename(columns={'Файл протокола': 'Кол-во стартов'}, inplace=True)
    df_not_started.sort_values(by='Кол-во стартов', ascending=False, inplace=True)
    today = datetime.date(datetime.now())
    rank_name = 'Не стартовавшие {} на '.format(application_config.rank_to_calculate) + str(today)
    df_not_started.to_excel(application_config.rank_dir / (rank_name + "_{}.xlsx".format(application_config.season)),
                            index=False)
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
