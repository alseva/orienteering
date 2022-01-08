import pandas as pd
from openpyxl import load_workbook


class ApplicationConfig:
    def __init__(self, excel_file: str):
        self._excel_file = excel_file

        self.protocol_source_type = None
        self.protocols_dir = None
        self.race_of_the_top_protocols_dir = None
        self.previous_year_final_rank_file = None
        self._load_main_settings()

        self.protocol_urls_df: pd.DataFrame = None
        self.mapping_yob_df: pd.DataFrame = None

    def _load_main_settings(self):
        wb = load_workbook(self._excel_file, read_only=True, keep_vba=False)
        main_settings = dict(wb['Настройки приложения'].values)
        self.protocol_source_type = main_settings['Тип источника протоколов']
        self.protocols_dir = main_settings['Путь к папке со всеми протоколами']
        self.race_of_the_top_protocols_dir = main_settings['Путь к папке с протоколами для гонки сильнейших']
        self.previous_year_final_rank_file = main_settings['Путь к файлу с финальным рангом предыдущего года']


if __name__ == '__main__':
    ApplicationConfig('C:\Alex\Projects\Python\Orienteering\Конфигуратор приложения.xlsx')
