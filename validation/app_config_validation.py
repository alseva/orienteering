from pathlib import Path
from typing import Generator

import pandas as pd
from openpyxl import load_workbook, Workbook

from constants import APP_CONFIG_FILE, APP_CONFIG_MAIN_SETTINGS_SHEET, APP_CONFIG_URLS_TO_PROTOCOLS_SHEET
from errors import Error, AppConfigValidationError


def check_app_config():
    check_app_config_exists()
    wb: Workbook = load_workbook(APP_CONFIG_FILE, read_only=True, keep_vba=False)
    check_workbook(wb)


def check_app_config_exists():
    if not Path(APP_CONFIG_FILE).exists():
        raise Error('Excel file with application configuration was not found.')


def check_workbook(wb: Workbook):
    if APP_CONFIG_MAIN_SETTINGS_SHEET not in wb.sheetnames:
        raise AppConfigValidationError(
            f'Sheet "{APP_CONFIG_MAIN_SETTINGS_SHEET}" was not found.')
    main_settings = dict(wb[APP_CONFIG_MAIN_SETTINGS_SHEET].values)

    if 'Тип источника протоколов ' not in main_settings:
        raise AppConfigValidationError('Field "Тип источника протоколов" was not found.')
    protocol_source_type = main_settings['Тип источника протоколов']
    if protocol_source_type not in ('Файл', 'Ссылка'):
        raise AppConfigValidationError(f'Unsupported protocol source type: "{protocol_source_type}".')

    if 'Путь к папке со всеми протоколами' not in main_settings:
        raise AppConfigValidationError('Field "Путь к папке со всеми протоколами" was not found.')
    protocols_dir = Path(main_settings['Путь к папке со всеми протоколами'])
    if protocol_source_type == 'Файл':
        if not protocols_dir.is_dir():
            raise AppConfigValidationError(f'Directory with protocols does not exist: "{protocols_dir}".')
        if not any(protocols_dir.glob('*.htm')):
            raise AppConfigValidationError(f'Directory "{protocols_dir}" does not contain any protocols (.htm files).')

    if protocol_source_type == 'Ссылка':
        check_urls_to_protocols(wb)

    # TODO: Check 'Путь к папке с протоколами для гонки сильнейших'
    # TODO: Check 'Путь к файлу с финальным рангом предыдущего года'
    # TODO: Check 'Путь к папке с результатами'


def check_urls_to_protocols(wb: Workbook):
    if APP_CONFIG_URLS_TO_PROTOCOLS_SHEET not in wb.sheetnames:
        raise AppConfigValidationError(
            f'Sheet "{APP_CONFIG_URLS_TO_PROTOCOLS_SHEET}" was not found.')

    values: Generator = wb[APP_CONFIG_URLS_TO_PROTOCOLS_SHEET].values
    header = next(values)
    if header != ('№ п/п', 'Ссылка', 'Учитывать при расчете ранга для Гонки сильнейших'):
        raise AppConfigValidationError(
            f'Unsupported header on the "{APP_CONFIG_URLS_TO_PROTOCOLS_SHEET}" sheet: {header}.')

    df = pd.DataFrame(values, columns=header)
    if df.empty:
        raise AppConfigValidationError(f'The "{APP_CONFIG_URLS_TO_PROTOCOLS_SHEET}" sheet does not contain any rows.')

    for column in ('№ п/п', 'Ссылка'):
        if not df[column].is_unique:
            raise AppConfigValidationError(f'The column "{column}" contains duplicated values on the '
                                           f'"{APP_CONFIG_URLS_TO_PROTOCOLS_SHEET}" sheet.')
