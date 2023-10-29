"""
This script scans CSV, XLX/XLSX, PARQUET files from input location defined in configuration file.

Configuration file location:    s3://iris-etl-framework/data/raw/FileIngestion_Config.ini

"""


import configparser
import glob
import os
from data_profiler_process.process_scanned_csv_data_profile import *
from data_profiler_process.process_scanned_excel_data_profile import *
from data_profiler_process.process_scanned_parquet_data_profile import *

def init_scan_files():
    """
    This function will read CSV, XLX/XLSX and PARQUET   file formats only !
    """
    pass


    read_config =configparser.ConfigParser()
    read_config.read('FileIngestion_Config_local.ini')
    DEFAULT_SECTION = read_config['DEFAULT']

    file_ingestion_file_source = DEFAULT_SECTION['file_ingestion_file_source']

    csv_source_file_full_path_dict={}
    parquet_source_file_full_path_dict={}
    excel_source_file_full_path_dict={}

    for file in glob.glob(file_ingestion_file_source+'\*.csv',recursive = True):
        csv_source_file_full_path_dict[os.path.basename(file)] = file

    for file in glob.glob(file_ingestion_file_source+'\*.xls*',recursive = True):
        excel_source_file_full_path_dict[os.path.basename(file)] = file

    for file in glob.glob(file_ingestion_file_source+'\*.parquet',recursive = True):
        parquet_source_file_full_path_dict[os.path.basename(file)] = file


    if csv_source_file_full_path_dict:
        process_scanned_csv_data_profile(source_file_full_path_dict=csv_source_file_full_path_dict)
    else:
        print('##USER_LOG## process_scanned_csv_data_profile: dict is empty')

    if excel_source_file_full_path_dict:
        process_scanned_excel_data_profile(source_file_full_path_dict=excel_source_file_full_path_dict)
    else:
        print('##USER_LOG## process_scanned_excel_data_profile: dict is empty')

    if parquet_source_file_full_path_dict:
        process_scanned_parquet_data_profile(source_file_full_path_dict=parquet_source_file_full_path_dict)
    else:
        print('##USER_LOG## process_scanned_parquet_data_profile: dict is empty')


