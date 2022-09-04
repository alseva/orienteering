from decimal import *

import pandas as pd
from openpyxl import load_workbook

# getcontext().prec = 28

from constants import RANK_CONFIG_MAIN_SETTINGS_SHEET, RANK_CONFIG_RACE_TYPE_SHEET, RANK_CONFIG_RACE_LEVEL_SHEET, \
    RANK_CONFIG_GROUP_RANK_SHEET, RANK_CONFIG_PENALTY_LACK_RACES_SHEET, RANK_CONFIG_PENALTY_NOT_STARTED_SHEET, \
    RANK_CONFIG_PENALTY_LEFT_RACE_SHEET, RANK_CONFIG_FILE


def get_decimals(s):
    return Decimal(s)


class RankFormulaConfig:
    def __init__(self, excel_file: str):
        self._excel_file = excel_file
        self._workbook = load_workbook(self._excel_file, read_only=True, keep_vba=False)

        self.race_percentage_for_final_rank = None
        self.race_number_to_start_apply_rules = None
        self.race_number_to_start_apply_rules_forest_rank = None
        self.race_number_to_start_apply_rules_sprint_rank = None
        self.race_number_to_start_apply_relative_rank = None
        self.race_number_to_start_apply_relative_rank_forest = None
        self.race_number_to_start_apply_relative_rank_sprint = None
        self.race_percentage_to_reset_final_rank = None
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
        main_settings = list(self._workbook[RANK_CONFIG_MAIN_SETTINGS_SHEET].values)
        self.race_percentage_for_final_rank = main_settings[1][1]
        self.race_number_to_start_apply_rules = main_settings[2][1]
        self.race_number_to_start_apply_rules_forest_rank = main_settings[3][1]
        self.race_number_to_start_apply_rules_sprint_rank = main_settings[4][1]
        self.race_number_to_start_apply_relative_rank = main_settings[5][1]
        self.race_number_to_start_apply_relative_rank_forest = main_settings[6][1]
        self.race_number_to_start_apply_relative_rank_sprint = main_settings[7][1]
        self.race_percentage_to_reset_final_rank = main_settings[8][1]

    def _load_race_type_df(self):
        race_types = list(self._workbook[RANK_CONFIG_RACE_TYPE_SHEET].values)
        self.race_type_df = pd.DataFrame(race_types[1:], columns=race_types[0])

    def _load_race_level_df(self):
        race_levels = list(self._workbook[RANK_CONFIG_RACE_LEVEL_SHEET].values)
        self.race_level_df = pd.DataFrame(race_levels[1:], columns=race_levels[0])

    def _load_group_rank_df(self):
        group_ranks = list(self._workbook[RANK_CONFIG_GROUP_RANK_SHEET].values)

        df_group_rank_m = pd.DataFrame(group_ranks[1:], columns=group_ranks[0])
        df_group_rank_m = df_group_rank_m.astype({'Возрастная группа': 'string', 'Ранг группы': 'int'})
        df_group_rank_m['Возрастная группа'] = 'М' + df_group_rank_m['Возрастная группа']

        df_group_rank_f = pd.DataFrame(group_ranks[1:], columns=group_ranks[0])
        df_group_rank_f = df_group_rank_f.astype({'Возрастная группа': 'string', 'Ранг группы': 'int'})
        df_group_rank_f['Возрастная группа'] = 'Ж' + df_group_rank_f['Возрастная группа']

        self.group_rank_df = pd.concat([df_group_rank_m, df_group_rank_f])

    def _load_penalty_lack_races_df(self):
        penalty_lack_races = list(self._workbook[RANK_CONFIG_PENALTY_LACK_RACES_SHEET].values)
        self.penalty_lack_races_df = pd.DataFrame(penalty_lack_races[1:], columns=penalty_lack_races[0])
        self.penalty_lack_races_df['Штраф за отсутствующие старты'] = self.penalty_lack_races_df[
                                                                          'Штраф за отсутствующие старты'] * 100
        self.penalty_lack_races_df['Штраф за отсутствующие старты'] = (self.penalty_lack_races_df[
                                                                           'Штраф за отсутствующие старты'].apply(
            get_decimals) / 100)

    def _load_penalty_not_started_df(self):
        penalty_not_started = list(self._workbook[RANK_CONFIG_PENALTY_NOT_STARTED_SHEET].values)
        self.penalty_not_started_df = pd.DataFrame(penalty_not_started[1:], columns=penalty_not_started[0])

    def _load_penalty_left_race_df(self):
        penalty_left_race = list(self._workbook[RANK_CONFIG_PENALTY_LEFT_RACE_SHEET].values)
        self.penalty_left_race_df = pd.DataFrame(penalty_left_race[1:], columns=penalty_left_race[0])


if __name__ == '__main__':
    rank_config = RankFormulaConfig(RANK_CONFIG_FILE)
    print(rank_config.race_number_to_start_apply_rules)
