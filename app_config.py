import pandas as pd
from openpyxl import load_workbook


class ApplicationConfig:
    def __init__(self, excel_file: str):
        self._excel_file = excel_file
        self._workbook = load_workbook(self._excel_file, read_only=True, keep_vba=False)

        self.protocol_source_type = None
        self.protocols_dir = None
        self.race_of_the_top_protocols_dir = None
        self.previous_year_final_rank_file = None
        self._load_main_settings()

        self.protocol_urls_df: pd.DataFrame = None
        self._load_protocol_urls_df()

        self.mapping_yob_df: pd.DataFrame = None
        self._load_mapping_yob_df()

    def _load_main_settings(self):
        main_settings = dict(self._workbook['Настройки приложения'].values)
        self.protocol_source_type = main_settings['Тип источника протоколов']
        self.protocols_dir = main_settings['Путь к папке со всеми протоколами']
        self.race_of_the_top_protocols_dir = main_settings['Путь к папке с протоколами для гонки сильнейших']
        self.previous_year_final_rank_file = main_settings['Путь к файлу с финальным рангом предыдущего года']

    def _load_protocol_urls_df(self):
        protocol_urls = list(self._workbook['Ссылки на протоколы'].values)
        self.protocol_urls_df = pd.DataFrame(protocol_urls[1:], columns=protocol_urls[0])

    def _load_mapping_yob_df(self):
        mapping_yob = list(self._workbook['Маппинг. Год рождения'].values)
        self.mapping_yob_df = pd.DataFrame(mapping_yob[1:], columns=mapping_yob[0])


if __name__ == '__main__':
    ApplicationConfig('C:\Alex\Projects\Python\Orienteering\Конфигуратор приложения.xlsx')
