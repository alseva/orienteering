import logging
import pandas as pd
import numpy as np
from decimal import *
from datetime import datetime

from app_config import ApplicationConfig


def save_current_rank(application_config: ApplicationConfig, current_rank_df: pd.DataFrame):
    final_rank_date = current_rank_df['Дата текущего соревнования'].max()
    final_rank_year = current_rank_df['Дата текущего соревнования'].max().year

    current_rank_df.dropna(subset=['Текущий ранг'], inplace=True)
    current_rank_df.index.name = '№ М+Ж'
    current_rank_df['Участник'] = current_rank_df['Фамилия'] + ' ' + current_rank_df['Имя']

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
        final_rank_name = 'Итоговый ранг сезона {}'.format(final_rank_year)
        columns['Итоговый ранг'] = final_rank_name
        fields_to_save.append(final_rank_name)
        fields_to_highlight.append(final_rank_name)

    current_rank_df.rename(columns=columns, inplace=True)
    current_rank_df = current_rank_df[fields_to_save]

    current_rank_df['Штраф'] = ((100 * (Decimal(1) - current_rank_df['Штраф'])).map(lambda x: round(x)) \
                                                                              .astype(str) + '%').replace('0%', '-')
    current_rank_df['Г.р.'] = current_rank_df['Г.р.'].replace(0, '-')

    for col in fields_to_highlight:
        current_rank_df[col] = current_rank_df[col].map(lambda x: round(x, 2))


    # Совмещенный ранг спортсменов (М+Ж)
    def highlight_col(x):
        df = x.copy()
        female_mask = df['Пол'] == 'Ж'
        df.loc[female_mask, :] = 'background-color: pink'
        df.loc[~female_mask, :] = 'background-color: lightblue'
        return df

    current_rank_df_mix = current_rank_df.copy()
    current_rank_df_mix = current_rank_df_mix.style.apply(highlight_col, axis=None,
                                                          subset=['Участник', 'Г.р.', 'Пол', rank_name])
    current_rank_df_mix.to_excel(
        application_config.rank_dir / (rank_name + " цветной_{}.xlsx".format(application_config.season)))


    # Ранги спортсменов отдельно: Мужчины, Женщины
    def format_and_save_rank(current_rank_df_gender, gender):
        current_rank_df_gender.reset_index(drop=False, inplace=True)
        current_rank_df_gender.index += 1
        current_rank_df_gender.index.name = '№'
        current_rank_df_gender.drop(labels='Пол', axis=1, inplace=True)
        current_rank_df_gender = current_rank_df_gender.style.background_gradient(cmap=application_config.rank_color,
                                                                          subset=fields_to_highlight)
        current_rank_df_gender.to_excel(
            application_config.rank_dir / (rank_name + " " + gender +"_{}.xlsx".format(application_config.season)))
        pass


    format_and_save_rank(current_rank_df[current_rank_df['Пол'] == 'М'].copy(), 'мужчины')
    format_and_save_rank(current_rank_df[current_rank_df['Пол'] == 'Ж'].copy(), 'женщины')

    # Совмещенный ранг спортсменов (М+Ж) БЕЗ раскраски
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
