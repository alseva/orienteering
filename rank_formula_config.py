import pandas as pd
from openpyxl import load_workbook


class RankFormulaConfig:
    def __init__(self, excel_file: str):
        self._excel_file = excel_file
        self._workbook = load_workbook(self._excel_file, read_only=True, keep_vba=False)

        self.race_percentage_for_final_rank = None
        self._load_main_settings()

        self.race_type_df: pd.DataFrame = None
        self._load_race_type_df()
        self.race_level_df: pd.DataFrame = None
        self._load_race_level_df()
        self.group_rank_df: pd.DataFrame = None
        self._load_group_rank_df()
        self.penalty_lack_races_df: pd.DataFrame = None
        self._load_penalty_lack_races_df()
        self.penalty_not_started_df: pd.DataFrame = None
        self._load_penalty_not_started_df()
        self.penalty_left_race_df: pd.DataFrame = None
        self._load_penalty_left_race_df()

    def _load_main_settings(self):
        main_settings = list(self._workbook['Общие настройки'].values)
        self.race_percentage_for_final_rank = main_settings[1][1]

    def _load_race_type_df(self):
        race_types = list(self._workbook['Коэффициент вида старта'].values)
        self.race_type_df = pd.DataFrame(race_types[1:], columns=race_types[0])

    def _load_race_level_df(self):
        race_levels = list(self._workbook['Коэффициент уровня старта'].values)
        self.race_level_df = pd.DataFrame(race_levels[1:], columns=race_levels[0])

    def _load_group_rank_df(self):
        group_ranks = list(self._workbook['Ранг группы'].values)
        self.group_rank_df = pd.DataFrame(group_ranks[1:], columns=group_ranks[0])

    def _load_penalty_lack_races_df(self):
        penalty_lack_races = list(self._workbook['Штраф за отсутствие старта'].values)
        self.penalty_lack_races_df = pd.DataFrame(penalty_lack_races[1:], columns=penalty_lack_races[0])

    def _load_penalty_not_started_df(self):
        penalty_not_started = list(self._workbook['Штраф за не стартовал'].values)
        self.penalty_not_started_df = pd.DataFrame(penalty_not_started[1:], columns=penalty_not_started[0])

    def _load_penalty_left_race_df(self):
        penalty_left_race = list(self._workbook['Штраф за снят'].values)
        self.penalty_left_race_df = pd.DataFrame(penalty_left_race[1:], columns=penalty_left_race[0])


if __name__ == '__main__':
    RankFormulaConfig('C:\Alex\Projects\Python\Orienteering\Конфигуратор формулы ранга.xlsx')
