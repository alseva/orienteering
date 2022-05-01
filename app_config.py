from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

from constants import APP_CONFIG_MAIN_SETTINGS_SHEET, APP_CONFIG_URLS_TO_PROTOCOLS_SHEET, APP_CONFIG_FILE


class ApplicationConfig:
    def __init__(self, excel_file: str):
        self._excel_file = excel_file
        self._workbook = load_workbook(self._excel_file, read_only=True, keep_vba=False)

        self.rank_to_calculate = None
        self.protocol_source_type = None
        self.protocols_dir: Path = None
        self.race_of_the_top_protocols_dir = None
        self.previous_year_final_rank_file = None
        self.rank_dir: Path = None
        self.rank_color = None
        self._load_main_settings()

        self.protocol_urls_df: pd.DataFrame = None
        self._load_protocol_urls_df()

        self.mapping_yob_df: pd.DataFrame = None
        self._load_mapping_yob_df()

        self.mapping_correct_participant_data: pd.DataFrame = None
        self._load_mapping_correct_participant_data_df()

        self.mapping_group_df: pd.DataFrame = None
        self._load_mapping_group_df()

    def _load_main_settings(self):
        main_settings = dict(value[:2] for value in self._workbook[APP_CONFIG_MAIN_SETTINGS_SHEET].values)

        self.protocol_source_type = main_settings['Тип источника протоколов']

        self.rank_to_calculate = main_settings['Ранг для расчета']
        if self.rank_to_calculate == 'Общий летний ранг':
            self.protocols_dir = main_settings['Путь к папке со всеми протоколами для Общего летнего ранга']
            self.previous_year_final_rank_file = main_settings[
                'Путь к файлу с финальным Общим летним рангом предыдущего года']
            self.rank_dir = main_settings['Путь к папке с результатами Общего летнего ранга']
            self.rank_color = main_settings['Цвет Общего летнего ранга']
        elif self.rank_to_calculate == 'Общий зимний ранг':
            self.protocols_dir = main_settings['Путь к папке со всеми протоколами для Общего зимнего ранга']
            self.previous_year_final_rank_file = main_settings[
                'Путь к файлу с финальным Общим зимним рангом предыдущего года']
            self.rank_dir = main_settings['Путь к папке с результатами Общего зимнего ранга']
            self.rank_color = main_settings['Цвет Общего зимнего ранга']
        elif self.rank_to_calculate == 'Лесной ранг':
            self.protocols_dir = main_settings['Путь к папке со всеми протоколами для Лесного ранга']
            self.previous_year_final_rank_file = main_settings[
                'Путь к файлу с финальным Лесным рангом предыдущего года']
            self.rank_dir = main_settings['Путь к папке с результатами Лесного ранга']
            self.rank_color = main_settings['Цвет Лесного ранга']
        elif self.rank_to_calculate == 'Спринт ранг':
            self.protocols_dir = main_settings['Путь к папке со всеми протоколами для Спринт ранга']
            self.previous_year_final_rank_file = main_settings[
                'Путь к файлу с финальным Спринт рангом предыдущего года']
            self.rank_dir = main_settings['Путь к папке с результатами Спринт ранга']
            self.rank_color = main_settings['Цвет Спринт ранга']
        else:
            self.protocols_dir = main_settings['Путь к папке с протоколами для гонки сильнейших']
            self.previous_year_final_rank_file = main_settings[
                'Путь к файлу с финальным рангом Гонки сильнейших предыдущего года']
            self.rank_dir = main_settings['Путь к папке с результатами ранга Гонки сильнейших']
            self.rank_color = main_settings['Цвет ранга Гонки сильнейших']
        self.protocols_dir = Path(self.protocols_dir)
        self.rank_dir = Path(self.rank_dir)
    def _load_protocol_urls_df(self):
        protocol_urls = list(self._workbook[APP_CONFIG_URLS_TO_PROTOCOLS_SHEET].values)
        self.protocol_urls_df = pd.DataFrame(protocol_urls[1:], columns=protocol_urls[0])

    def _load_mapping_yob_df(self):
        mapping_yob = list(self._workbook['Маппинг. Год рождения'].values)
        self.mapping_yob_df = pd.DataFrame(mapping_yob[1:], columns=mapping_yob[0])

    def _load_mapping_correct_participant_data_df(self):
        mapping_name = list(self._workbook['Маппинг. ФИО'].values)
        self.mapping_correct_participant_data = pd.DataFrame(mapping_name[1:], columns=mapping_name[0])

    def _load_mapping_group_df(self):
        mapping_group = list(self._workbook['Маппинг. Возрастная группа'].values)
        self.mapping_group_df = pd.DataFrame(mapping_group[1:], columns=mapping_group[0])


if __name__ == '__main__':
    ApplicationConfig(APP_CONFIG_FILE)
