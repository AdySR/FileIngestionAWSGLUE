import glob2 as glob
import pandas as pd
import ydata_profiling as pp
import numpy as np
import json
from datetime import datetime
import configparser
import xlrd
import openpyxl
import pyarrow
import fastparquet
import awswrangler as wr

import os
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from awsglue.dynamicframe import DynamicFrame
import boto3


#Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)


def process_scanned_csv_data_profile(source_file_full_path_dict,data_profile_output):

    run_date = datetime.today().strftime('%Y%m%d')
    #output_location also needs to be dynamically calculated. 
    output_location = 's3://iris-etl-framework/data/raw/data_profile_output/'
    
    for file_name, file_full_path in source_file_full_path_dict.items():
        file_name_without_ext =os.path.splitext(file_name)[0]
        input_df = pd.read_csv(file_full_path)
        profile = pp.ProfileReport(input_df, title="Data Profile Report",  minimal = True)

        profile.to_file(f'./{file_name_without_ext}_csv_profile.html')
        wr.s3.upload(local_file=f'./{file_name_without_ext}_csv_profile.html', path=f'{output_location}{file_name_without_ext}_{run_date}_csv_profile.html')
        os.remove(f'./{file_name_without_ext}_csv_profile.html')
    
        
        json_obj = json.loads(profile.to_json())
        df = pd.DataFrame(json_obj["variables"])
        df = df.drop(index = ['value_counts_without_nan', 'value_counts_index_sorted', 'hashable', 'ordering', 'n', 'first_rows', 'memory_size', 'n_negative', 'p_negative', 'n_infinite', 'n_zeros', 'sum', 'histogram', 'p_zeros', 'p_infinite', 'monotonic_increase', 'monotonic_decrease', 'monotonic_increase_strict', 'monotonic_decrease_strict'])
        
        df = df.transpose()
        df.rename(columns = {'count':'No. of Values', 'n_distinct':'Distinct', 'p_distinct':'Distinct (%)','is_unique':'Is Unique?','n_unique':'Unique', 'p_unique':'Unique (%)', 'type':'Data Type', 'n_missing':'Missing', 'p_missing':'Missing (%)', 'mean':'Mean', 'std':'Standard deviation', 'variance':'Variance', 'min':'Minimum', 'max':'Maximum', 'kurtosis':'Kurtosis', 'skewness':'Skewness', 'mad':'Median Absolute Deviation (MAD)', 'range':'Range', '5%':'5-th percentile', '25%':'Q1', '50%':'Median', '75%':'Q3', '95%':'95-th percentile', 'iqr':'Interquartile range (IQR)', 'cv':'Coefficient of variation (CV)', 'monotonic':'Monotonicity'}, inplace = True)
        df['Column Name'] = df.index
        column_titles = ['Column Name', 'Data Type', 'No. of Values', 'Distinct', 'Distinct (%)', 'Is Unique?', 'Unique', 'Unique (%)',  'Missing', 'Missing (%)', 'Mean', 'Standard deviation', 'Variance', 'Minimum', 'Maximum', 'Kurtosis', 'Skewness', 'Median Absolute Deviation (MAD)', 'Range', '5-th percentile', 'Q1', 'Median', 'Q3', '95-th percentile', 'Interquartile range (IQR)', 'Coefficient of variation (CV)', 'Monotonicity']
        df = df.reindex(columns=column_titles)
        
        df.insert(0, 'File Name', file_name)
    
        df['Distinct (%)'] = (df['Distinct (%)']*100).astype(float).round(2)
        df['Unique (%)'] = (df['Unique (%)']*100).astype(float).round(2)
        df['Missing (%)'] = (df['Missing (%)']*100).astype(float).round(2)
    
        df['Mean'] = df['Mean'].astype(float).round(2)
        df['Standard deviation'] = df['Standard deviation'].astype(float).round(2)
        df['Variance'] = df['Mean'].astype(float).round(2)
        df['Minimum'] = df['Minimum'].astype(float).round(2)
        df['Maximum'] = df['Maximum'].astype(float).round(2)
        df['Kurtosis'] = df['Kurtosis'].astype(float).round(2)
        df['Skewness'] = df['Skewness'].astype(float).round(2)
        df['Median Absolute Deviation (MAD)'] = df['Median Absolute Deviation (MAD)'].astype(float).round(2)
        df['5-th percentile'] = df['5-th percentile'].astype(float).round(2)
        df['Q1'] = df['Q1'].astype(float).round(2)
        df['Median'] = df['Median'].astype(float).round(2)
        df['Q3'] = df['Q3'].astype(float).round(2)
        df['95-th percentile'] = df['95-th percentile'].astype(float).round(2)
        df['Median Absolute Deviation (MAD)'] = df['Median Absolute Deviation (MAD)'].astype(float).round(2)
        df['5-th percentile'] = df['5-th percentile'].astype(float).round(2)
        df['Interquartile range (IQR)'] = df['Interquartile range (IQR)'].astype(float).round(2)
        df['Coefficient of variation (CV)'] = df['Coefficient of variation (CV)'].astype(float).round(2)
        df['Monotonicity'] = df['Monotonicity'].astype(float).round(2)
    
        # rename data types as per glue catalog data types
        data_dict = {
        'object': 'string',
        'int32': 'int',
        'int64': 'bigint',
        'float64': 'double'
        }
        
        wr.s3.to_csv(df, path=f'{output_location}{file_name_without_ext}_{run_date}_csv_profile.csv', index=False)
        
        




def process_scanned_parquet_data_profile(source_file_full_path_dict,data_profile_output):

    run_date = datetime.today().strftime('%Y%m%d')
    #output_location also needs to be dynamically calculated. 
    output_location = 's3://iris-etl-framework/data/raw/data_profile_output/'
    
    for file_name, file_full_path in source_file_full_path_dict.items():
        file_name_without_ext =os.path.splitext(file_name)[0]
        input_df = pd.read_parquet(file_full_path)
        profile = pp.ProfileReport(input_df, title="Data Profile Report",  minimal = True)

        profile.to_file(f'./{file_name_without_ext}_parquet_profile.html')
        wr.s3.upload(local_file=f'./{file_name_without_ext}_parquet_profile.html', path=f'{output_location}{file_name_without_ext}_{run_date}_parquet_profile.html')
        os.remove(f'./{file_name_without_ext}_parquet_profile.html')
    
        
        json_obj = json.loads(profile.to_json())
        df = pd.DataFrame(json_obj["variables"])
        df = df.drop(index = ['value_counts_without_nan', 'value_counts_index_sorted', 'hashable', 'ordering', 'n', 'first_rows', 'memory_size', 'n_negative', 'p_negative', 'n_infinite', 'n_zeros', 'sum', 'histogram', 'p_zeros', 'p_infinite', 'monotonic_increase', 'monotonic_decrease', 'monotonic_increase_strict', 'monotonic_decrease_strict'])
        
        df = df.transpose()
        df.rename(columns = {'count':'No. of Values', 'n_distinct':'Distinct', 'p_distinct':'Distinct (%)','is_unique':'Is Unique?','n_unique':'Unique', 'p_unique':'Unique (%)', 'type':'Data Type', 'n_missing':'Missing', 'p_missing':'Missing (%)', 'mean':'Mean', 'std':'Standard deviation', 'variance':'Variance', 'min':'Minimum', 'max':'Maximum', 'kurtosis':'Kurtosis', 'skewness':'Skewness', 'mad':'Median Absolute Deviation (MAD)', 'range':'Range', '5%':'5-th percentile', '25%':'Q1', '50%':'Median', '75%':'Q3', '95%':'95-th percentile', 'iqr':'Interquartile range (IQR)', 'cv':'Coefficient of variation (CV)', 'monotonic':'Monotonicity'}, inplace = True)
        df['Column Name'] = df.index
        column_titles = ['Column Name', 'Data Type', 'No. of Values', 'Distinct', 'Distinct (%)', 'Is Unique?', 'Unique', 'Unique (%)',  'Missing', 'Missing (%)', 'Mean', 'Standard deviation', 'Variance', 'Minimum', 'Maximum', 'Kurtosis', 'Skewness', 'Median Absolute Deviation (MAD)', 'Range', '5-th percentile', 'Q1', 'Median', 'Q3', '95-th percentile', 'Interquartile range (IQR)', 'Coefficient of variation (CV)', 'Monotonicity']
        df = df.reindex(columns=column_titles)
        
        df.insert(0, 'File Name', file_name)
    
        df['Distinct (%)'] = (df['Distinct (%)']*100).astype(float).round(2)
        df['Unique (%)'] = (df['Unique (%)']*100).astype(float).round(2)
        df['Missing (%)'] = (df['Missing (%)']*100).astype(float).round(2)
    
        df['Mean'] = df['Mean'].astype(float).round(2)
        df['Standard deviation'] = df['Standard deviation'].astype(float).round(2)
        df['Variance'] = df['Mean'].astype(float).round(2)
        df['Minimum'] = df['Minimum'].astype(float).round(2)
        df['Maximum'] = df['Maximum'].astype(float).round(2)
        df['Kurtosis'] = df['Kurtosis'].astype(float).round(2)
        df['Skewness'] = df['Skewness'].astype(float).round(2)
        df['Median Absolute Deviation (MAD)'] = df['Median Absolute Deviation (MAD)'].astype(float).round(2)
        df['5-th percentile'] = df['5-th percentile'].astype(float).round(2)
        df['Q1'] = df['Q1'].astype(float).round(2)
        df['Median'] = df['Median'].astype(float).round(2)
        df['Q3'] = df['Q3'].astype(float).round(2)
        df['95-th percentile'] = df['95-th percentile'].astype(float).round(2)
        df['Median Absolute Deviation (MAD)'] = df['Median Absolute Deviation (MAD)'].astype(float).round(2)
        df['5-th percentile'] = df['5-th percentile'].astype(float).round(2)
        df['Interquartile range (IQR)'] = df['Interquartile range (IQR)'].astype(float).round(2)
        df['Coefficient of variation (CV)'] = df['Coefficient of variation (CV)'].astype(float).round(2)
        df['Monotonicity'] = df['Monotonicity'].astype(float).round(2)
    
        # rename data types as per glue catalog data types
        data_dict = {
        'object': 'string',
        'int32': 'int',
        'int64': 'bigint',
        'float64': 'double'
        }
        
        wr.s3.to_csv(df, path=f'{output_location}{file_name_without_ext}_{run_date}_parquet_profile.csv', index=False)
        



def process_scanned_excel_data_profile(source_file_full_path_dict,data_profile_output):

    run_date = datetime.today().strftime('%Y%m%d')
    #output_location also needs to be dynamically calculated. 
    output_location = 's3://iris-etl-framework/data/raw/data_profile_output/'
    
    for file_name, file_full_path in source_file_full_path_dict.items():
        file_name_without_ext =os.path.splitext(file_name)[0]
        input_df = pd.read_excel(file_full_path)

        profile = pp.ProfileReport(input_df, title="Data Profile Report",  minimal = True)

        profile.to_file(f'./{file_name_without_ext}_excel_profile.html')
        wr.s3.upload(local_file=f'./{file_name_without_ext}_excel_profile.html', path=f'{output_location}{file_name_without_ext}_{run_date}_excel_profile.html')
        os.remove(f'./{file_name_without_ext}_excel_profile.html')
    
        
        json_obj = json.loads(profile.to_json())
        df = pd.DataFrame(json_obj["variables"])
        df = df.drop(index = ['value_counts_without_nan', 'value_counts_index_sorted', 'hashable', 'ordering', 'n', 'first_rows', 'memory_size', 'n_negative', 'p_negative', 'n_infinite', 'n_zeros', 'sum', 'histogram', 'p_zeros', 'p_infinite', 'monotonic_increase', 'monotonic_decrease', 'monotonic_increase_strict', 'monotonic_decrease_strict'])
        
        df = df.transpose()
        df.rename(columns = {'count':'No. of Values', 'n_distinct':'Distinct', 'p_distinct':'Distinct (%)','is_unique':'Is Unique?','n_unique':'Unique', 'p_unique':'Unique (%)', 'type':'Data Type', 'n_missing':'Missing', 'p_missing':'Missing (%)', 'mean':'Mean', 'std':'Standard deviation', 'variance':'Variance', 'min':'Minimum', 'max':'Maximum', 'kurtosis':'Kurtosis', 'skewness':'Skewness', 'mad':'Median Absolute Deviation (MAD)', 'range':'Range', '5%':'5-th percentile', '25%':'Q1', '50%':'Median', '75%':'Q3', '95%':'95-th percentile', 'iqr':'Interquartile range (IQR)', 'cv':'Coefficient of variation (CV)', 'monotonic':'Monotonicity'}, inplace = True)
        df['Column Name'] = df.index
        column_titles = ['Column Name', 'Data Type', 'No. of Values', 'Distinct', 'Distinct (%)', 'Is Unique?', 'Unique', 'Unique (%)',  'Missing', 'Missing (%)', 'Mean', 'Standard deviation', 'Variance', 'Minimum', 'Maximum', 'Kurtosis', 'Skewness', 'Median Absolute Deviation (MAD)', 'Range', '5-th percentile', 'Q1', 'Median', 'Q3', '95-th percentile', 'Interquartile range (IQR)', 'Coefficient of variation (CV)', 'Monotonicity']
        df = df.reindex(columns=column_titles)
        df.insert(0, 'File Name', file_name)
        # change to percentage and round 2 decimal place
        df['Distinct (%)'] = (df['Distinct (%)']*100).astype(float).round(2)
        df['Unique (%)'] = (df['Unique (%)']*100).astype(float).round(2)
        df['Missing (%)'] = (df['Missing (%)']*100).astype(float).round(2)
        # round 2 decimal place
        df['Mean'] = df['Mean'].astype(float).round(2)
        df['Standard deviation'] = df['Standard deviation'].astype(float).round(2)
        df['Variance'] = df['Mean'].astype(float).round(2)
        # df['Minimum'] = df['Minimum'].astype(float).round(2)
        # df['Maximum'] = df['Maximum'].astype(float).round(2)
        df['Kurtosis'] = df['Kurtosis'].astype(float).round(2)
        df['Skewness'] = df['Skewness'].astype(float).round(2)
        df['Median Absolute Deviation (MAD)'] = df['Median Absolute Deviation (MAD)'].astype(float).round(2)
        df['5-th percentile'] = df['5-th percentile'].astype(float).round(2)
        df['Q1'] = df['Q1'].astype(float).round(2)
        df['Median'] = df['Median'].astype(float).round(2)
        df['Q3'] = df['Q3'].astype(float).round(2)
        df['95-th percentile'] = df['95-th percentile'].astype(float).round(2)
        df['Median Absolute Deviation (MAD)'] = df['Median Absolute Deviation (MAD)'].astype(float).round(2)
        df['5-th percentile'] = df['5-th percentile'].astype(float).round(2)
        df['Interquartile range (IQR)'] = df['Interquartile range (IQR)'].astype(float).round(2)
        df['Coefficient of variation (CV)'] = df['Coefficient of variation (CV)'].astype(float).round(2)
        df['Monotonicity'] = df['Monotonicity'].astype(float).round(2)

        # rename data types as per glue catalog data types
        data_dict = {
        'object': 'string',
        'int32': 'int',
        'int64': 'bigint',
        'float64': 'double'
        }

        wr.s3.to_csv(df, path=f'{output_location}{file_name_without_ext}_{run_date}_excel_profile.csv', index=False)





def init_scan_files():
    read_config =configparser.ConfigParser()
    read_config.read('FileIngestion_Config.ini')
    DEFAULT_SECTION = read_config['DEFAULT']
    
    storage = DEFAULT_SECTION['storage']
    source_bucket = DEFAULT_SECTION['source_bucket']
    dir_file_ingestion_source = DEFAULT_SECTION['file_ingestion_source']
    data_profile_output = DEFAULT_SECTION['data_profile_output']
    file_ingestion_source = f"{storage}://{source_bucket}/{dir_file_ingestion_source}"
    
    csv_source_file_full_path_dict={}
    parquet_source_file_full_path_dict={}
    excel_source_file_full_path_dict={}
    excel_options= ['xls','xlsx']
    
    
    s3 = boto3.resource(storage)
    my_bucket = s3.Bucket(source_bucket)
    
    for object_summary in my_bucket.objects.filter(Prefix=dir_file_ingestion_source):
        if object_summary.key.endswith('csv'):
            csv_source_file_full_path_dict[f"{storage}://{source_bucket}/{object_summary.key}".split("data/raw/file_ingestion_source/",1)[1]]= f"{storage}://{source_bucket}/{object_summary.key}"
    
    for object_summary in my_bucket.objects.filter(Prefix=dir_file_ingestion_source):
        if object_summary.key.endswith('parquet'):
            parquet_source_file_full_path_dict[f"{storage}://{source_bucket}/{object_summary.key}".split("data/raw/file_ingestion_source/",1)[1]]= f"{storage}://{source_bucket}/{object_summary.key}"
    
    for object_summary in my_bucket.objects.filter(Prefix=dir_file_ingestion_source):
        if any (object_summary.key.endswith(s) for s in excel_options):
            excel_source_file_full_path_dict[f"{storage}://{source_bucket}/{object_summary.key}".split("data/raw/file_ingestion_source/",1)[1]]= f"{storage}://{source_bucket}/{object_summary.key}"

    
    
    
    if csv_source_file_full_path_dict:
        process_scanned_csv_data_profile(source_file_full_path_dict=csv_source_file_full_path_dict,data_profile_output=data_profile_output)
    else:
        print('##USER_LOG## process_scanned_csv_data_profile: dict is empty')
        

    if parquet_source_file_full_path_dict:
        process_scanned_parquet_data_profile(source_file_full_path_dict=parquet_source_file_full_path_dict,data_profile_output=data_profile_output)
    else:
        print('##USER_LOG## process_scanned_parquet_data_profile: dict is empty')


    if excel_source_file_full_path_dict:
        process_scanned_excel_data_profile(source_file_full_path_dict=excel_source_file_full_path_dict,data_profile_output=data_profile_output)
    else:
        print('##USER_LOG## process_scanned_excel_data_profile: dict is empty')







if (__name__ =='__main__'):
    init_scan_files()




job.commit()
