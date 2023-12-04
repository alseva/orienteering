import logging
import math
import os
import re
import warnings
from decimal import *

getcontext().prec = 28

import numpy as np
import pandas as pd

from app_config import ApplicationConfig
from constants import APP_CONFIG_FILE, RANK_CONFIG_FILE, VERSION
from errors import Error
from logger import setup_logging
from rank_formula_config import RankFormulaConfig
from validation.app_config_validation import check_app_config
from validation.rank_config_validation import check_rank_config
from prepare_protocols import prepare_protocols, download_protocols
from save_current_rank import save_current_rank, transform_and_save_not_started_and_left_race


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
            df_previous_year_final_rank['Фамилия'] = df_previous_year_final_rank['Участник'].map(
                lambda x: str(x).split(' ')[0])
            df_previous_year_final_rank['Имя'] = df_previous_year_final_rank['Участник'].map(
                lambda x: str(x).split(' ')[1:])
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
                df[df['left_race'].isna()])  # исключаем снятных из кол-ва участников для расчета ранга соревнований
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

        # оставляем только строку по каждому участнику (с датой последнего соревнования, вошедшего в расчет ранга)
        def left_current_rank_only(df):
            df = df.sort_values(by='Дата соревнования', ascending=False).head(1)
            return df

        current_rank_df = current_rank_df.groupby(by=participant_fields, as_index=False).apply(left_current_rank_only)

        # добавляем текущий ранг к протоколу текущего соревнования
        # соединяем по участнику и соревнованию, по итогам которого расчитан последний текущий ранг
        # считаем, что в один день может быть только одно соревнование
        protocol_df = protocol_df.merge(current_rank_df,
                                        how='left',
                                        left_on=participant_fields + ['Дата соревнования'],
                                        right_on=participant_fields + ['Дата текущего соревнования'],
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


if __name__ == '__main__':
    setup_logging()
    logging.info(f'Rank calculator version {VERSION}\nAlex, Inc. No rights are reserved.\n')
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
