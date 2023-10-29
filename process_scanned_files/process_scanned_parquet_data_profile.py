import pandas as pd
import ydata_profiling as pp
import numpy as np
import boto3
import awswrangler as wr
import json
from datetime import datetime
import os
import configparser

# def process_scanned_parquet_data_profile(source_file_full_path_dict):


source_file_full_path_dict= {'Iris.parquet': 'C:\\programs\\pyprojects\\FileIngestionAWS\\file_ingestion_file_source\\Iris.parquet', 'mt cars.parquet': 'C:\\programs\\pyprojects\\FileIngestionAWS\\file_ingestion_file_source\\mt cars.parquet', 'Titanic.parquet': 'C:\\programs\\pyprojects\\FileIngestionAWS\\file_ingestion_file_source\\Titanic.parquet'} 
read_config =configparser.ConfigParser()
read_config.read('FileIngestion_Config_local.ini')
details_dict = dict(read_config.items('DEFAULT'))
DEFAULT_SECTION = read_config['DEFAULT']
run_date = datetime.today().strftime('%Y%m%d')

data_profile_output = DEFAULT_SECTION['data_profile_output']
for file_name, file_full_path in source_file_full_path_dict.items():
    file_name_without_ext =os.path.splitext(file_name)[0]
    print(file_name_without_ext)
    input_df = pd.read_parquet(file_full_path)
    print(input_df.head(5))

    # output_file_name_csv_profile = f'{data_profile_output}\{file_name_without_ext}_csv_profile.csv'
    # output_file_name_html_profile = f'{data_profile_output}\{file_name_without_ext}_html_profile.html'
    # profile = pp.ProfileReport(input_df, title="Data Profile Report",  minimal = True)
    # profile.to_file(output_file_name_html_profile)

    # json_obj = json.loads(profile.to_json())
    # df = pd.DataFrame(json_obj["variables"])
    # df = df.drop(index = ['value_counts_without_nan', 'value_counts_index_sorted', 'hashable', 'ordering', 'n', 'first_rows', 'memory_size', 'n_negative', 'p_negative', 'n_infinite', 'n_zeros', 'sum', 'histogram', 'p_zeros', 'p_infinite', 'monotonic_increase', 'monotonic_decrease', 'monotonic_increase_strict', 'monotonic_decrease_strict'])
    # df = df.transpose()
    # df.rename(columns = {'count':'No. of Values', 'n_distinct':'Distinct', 'p_distinct':'Distinct (%)','is_unique':'Is Unique?','n_unique':'Unique', 'p_unique':'Unique (%)', 'type':'Data Type', 'n_missing':'Missing', 'p_missing':'Missing (%)', 'mean':'Mean', 'std':'Standard deviation', 'variance':'Variance', 'min':'Minimum', 'max':'Maximum', 'kurtosis':'Kurtosis', 'skewness':'Skewness', 'mad':'Median Absolute Deviation (MAD)', 'range':'Range', '5%':'5-th percentile', '25%':'Q1', '50%':'Median', '75%':'Q3', '95%':'95-th percentile', 'iqr':'Interquartile range (IQR)', 'cv':'Coefficient of variation (CV)', 'monotonic':'Monotonicity'}, inplace = True)
    # df['Column Name'] = df.index
    # column_titles = ['Column Name', 'Data Type', 'No. of Values', 'Distinct', 'Distinct (%)', 'Is Unique?', 'Unique', 'Unique (%)',  'Missing', 'Missing (%)', 'Mean', 'Standard deviation', 'Variance', 'Minimum', 'Maximum', 'Kurtosis', 'Skewness', 'Median Absolute Deviation (MAD)', 'Range', '5-th percentile', 'Q1', 'Median', 'Q3', '95-th percentile', 'Interquartile range (IQR)', 'Coefficient of variation (CV)', 'Monotonicity']
    # df = df.reindex(columns=column_titles)
    # df.insert(0, 'File Name', file_name)
    
    # df['Distinct (%)'] = (df['Distinct (%)']*100).astype(float).round(2)
    # df['Unique (%)'] = (df['Unique (%)']*100).astype(float).round(2)
    # df['Missing (%)'] = (df['Missing (%)']*100).astype(float).round(2)

    # df['Mean'] = df['Mean'].astype(float).round(2)
    # df['Standard deviation'] = df['Standard deviation'].astype(float).round(2)
    # df['Variance'] = df['Mean'].astype(float).round(2)
    # df['Minimum'] = df['Minimum'].astype(float).round(2)
    # df['Maximum'] = df['Maximum'].astype(float).round(2)
    # df['Kurtosis'] = df['Kurtosis'].astype(float).round(2)
    # df['Skewness'] = df['Skewness'].astype(float).round(2)
    # df['Median Absolute Deviation (MAD)'] = df['Median Absolute Deviation (MAD)'].astype(float).round(2)
    # df['5-th percentile'] = df['5-th percentile'].astype(float).round(2)
    # df['Q1'] = df['Q1'].astype(float).round(2)
    # df['Median'] = df['Median'].astype(float).round(2)
    # df['Q3'] = df['Q3'].astype(float).round(2)
    # df['95-th percentile'] = df['95-th percentile'].astype(float).round(2)
    # df['Median Absolute Deviation (MAD)'] = df['Median Absolute Deviation (MAD)'].astype(float).round(2)
    # df['5-th percentile'] = df['5-th percentile'].astype(float).round(2)
    # df['Interquartile range (IQR)'] = df['Interquartile range (IQR)'].astype(float).round(2)
    # df['Coefficient of variation (CV)'] = df['Coefficient of variation (CV)'].astype(float).round(2)
    # df['Monotonicity'] = df['Monotonicity'].astype(float).round(2)


    # data_dict = {
    # 'object': 'string',
    # 'int32': 'int',
    # 'int64': 'bigint',
    # 'float64': 'double'
    # }

    # df.to_csv(output_file_name_csv_profile, index=False)