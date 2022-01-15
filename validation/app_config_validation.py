from pathlib import Path

from openpyxl import load_workbook, Workbook

from constants import APP_CONFIG_FILE, APP_CONFIG_MAIN_SETTINGS_SHEET, APP_CONFIG_URLS_TO_PROTOCOLS_SHEET
from validation.exceptions import AppException, AppConfigException


def check_app_config():
    check_app_config_exists()
    wb: Workbook = load_workbook(APP_CONFIG_FILE, read_only=True, keep_vba=False)
    check_main_settings(wb)
    check_urls_to_protocols(wb)


def check_app_config_exists():
    if not Path(APP_CONFIG_FILE).exists():
        raise AppException('Excel file with application configuration was not found.')


def check_main_settings(wb: Workbook):
    if APP_CONFIG_MAIN_SETTINGS_SHEET not in wb.sheetnames:
        raise AppConfigException(
            f'Sheet "{APP_CONFIG_MAIN_SETTINGS_SHEET}" was not found in the application configuration.')


def check_urls_to_protocols(wb: Workbook):
    if APP_CONFIG_URLS_TO_PROTOCOLS_SHEET not in wb.sheetnames:
        raise AppConfigException(
            f'Sheet "{APP_CONFIG_URLS_TO_PROTOCOLS_SHEET}" was not found in the application configuration.')
