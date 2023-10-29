# Author: <>

# data-profiler job is used to profile input data file 

# It has PII data detection feature.


import os
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from awsglue.dynamicframe import DynamicFrame


import pandas as pd
import ydata_profiling as pp
import numpy as np
import boto3
import awswrangler as wr
import json
from datetime import datetime

#Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME','FILE_NAME'])
job.init(args['JOB_NAME'], args)

# input file name from argument 
file_name = args['FILE_NAME']

file_name1 = file_name[:-4]

# input and output directory location
input_location = 's3://iris-etl-framework/data/raw/'
output_location = 's3://iris-etl-framework/data/profile/'

run_date = datetime.today().strftime('%Y%m%d')

# read input file from s3 location
input_df = wr.s3.read_csv(path=f'{input_location}{file_name}')

# collect column name and datatypes and convert to dict
df_dict=input_df.dtypes.to_dict()

#perform profiling
profile = pp.ProfileReport(input_df, title="Data Profile Report",  minimal = True)

# write .html file to s3 location
profile.to_file(f'./{file_name1}-profile.html')
wr.s3.upload(local_file=f'./{file_name1}-profile.html', path=f'{output_location}{file_name1}-{run_date}-profile.html')
os.remove(f'./{file_name1}-profile.html')
###

output_file_name = f"{file_name1}-{run_date}-profile.csv"

json_obj = json.loads(profile.to_json())
print('profile_type: {0} json_obj: {1}'.format(type(profile),type(json_obj)))
df = pd.DataFrame(json_obj["variables"])

df = df.drop(index = ['value_counts_without_nan', 'value_counts_index_sorted', 'hashable', 'ordering', 'n', 'first_rows', 'memory_size', 'n_negative', 'p_negative', 'n_infinite', 'n_zeros', 'sum', 'histogram', 'p_zeros', 'p_infinite', 'monotonic_increase', 'monotonic_decrease', 'monotonic_increase_strict', 'monotonic_decrease_strict'])

# transpose the dataframe
df = df.transpose()

#rename
df.rename(columns = {'count':'No. of Values', 'n_distinct':'Distinct', 'p_distinct':'Distinct (%)','is_unique':'Is Unique?','n_unique':'Unique', 'p_unique':'Unique (%)', 'type':'Data Type', 'n_missing':'Missing', 'p_missing':'Missing (%)', 'mean':'Mean', 'std':'Standard deviation', 'variance':'Variance', 'min':'Minimum', 'max':'Maximum', 'kurtosis':'Kurtosis', 'skewness':'Skewness', 'mad':'Median Absolute Deviation (MAD)', 'range':'Range', '5%':'5-th percentile', '25%':'Q1', '50%':'Median', '75%':'Q3', '95%':'95-th percentile', 'iqr':'Interquartile range (IQR)', 'cv':'Coefficient of variation (CV)', 'monotonic':'Monotonicity'}, inplace = True)

#convert index as column name
df['Column Name'] = df.index

# arrange column names as per business requirement
column_titles = ['Column Name', 'Data Type', 'No. of Values', 'Distinct', 'Distinct (%)', 'Is Unique?', 'Unique', 'Unique (%)',  'Missing', 'Missing (%)', 'Mean', 'Standard deviation', 'Variance', 'Minimum', 'Maximum', 'Kurtosis', 'Skewness', 'Median Absolute Deviation (MAD)', 'Range', '5-th percentile', 'Q1', 'Median', 'Q3', '95-th percentile', 'Interquartile range (IQR)', 'Coefficient of variation (CV)', 'Monotonicity']

df = df.reindex(columns=column_titles)

#insert file name column
df.insert(0, 'File Name', file_name)

# change to percentage and round 2 decimal place
df['Distinct (%)'] = (df['Distinct (%)']*100).astype(float).round(2)
df['Unique (%)'] = (df['Unique (%)']*100).astype(float).round(2)
df['Missing (%)'] = (df['Missing (%)']*100).astype(float).round(2)
# round 2 decimal place
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

for column_name, dtyp in df_dict.items():
    df['Data Type'] = np.where(df['Column Name'] == column_name, data_dict[str(dtyp)], df['Data Type'])

print('PII starts: {0}'.format(str(datetime.now())))
#PII 
# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": [f'{input_location}{file_name}']},
    transformation_ctx="S3bucket_node1",
)
# Script generated for node ApplyMapping
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    S3bucket_node1,
    [
        "PERSON_NAME",
        "EMAIL",
     #   "CREDIT_CARD",
     #   "IP_ADDRESS",
     #   "MAC_ADDRESS",
        "PHONE_NUMBER",
        "USA_PASSPORT_NUMBER",
        "USA_SSN",
        "USA_ITIN",
        "BANK_ACCOUNT",
        "USA_DRIVING_LICENSE",
     #   "USA_HCPCS_CODE",
     #   "USA_NATIONAL_DRUG_CODE",
     #   "USA_NATIONAL_PROVIDER_IDENTIFIER",
     #   "USA_DEA_NUMBER",
     #   "USA_HEALTH_INSURANCE_CLAIM_NUMBER",
     #   "USA_MEDICARE_BENEFICIARY_IDENTIFIER",
     #   "JAPAN_BANK_ACCOUNT",
     #   "JAPAN_DRIVING_LICENSE",
     #   "JAPAN_MY_NUMBER",
     #   "JAPAN_PASSPORT_NUMBER",
     #   "UK_BANK_ACCOUNT",
     #   "UK_BANK_SORT_CODE",
     #   "UK_DRIVING_LICENSE",
     #   "UK_ELECTORAL_ROLL_NUMBER",
     #   "UK_NATIONAL_HEALTH_SERVICE_NUMBER",
     #   "UK_NATIONAL_INSURANCE_NUMBER",
     #   "UK_PASSPORT_NUMBER",
     #   "UK_PHONE_NUMBER",
     #   "UK_UNIQUE_TAXPAYER_REFERENCE_NUMBER",
     #   "UK_VALUE_ADDED_TAX",
        "CANADA_SIN",
        "CANADA_PASSPORT_NUMBER",
        "GENDER",
    ],
    0.10,
    0.07,
)

#classification columns
df.insert(3, 'Classification', '')
df.insert(4, 'Classification Details', '')

for column_name, value in classified_map.items():
    df['Classification'] = np.where(df['Column Name'] == column_name, 'PII', df['Classification'])
    df['Classification Details'] = np.where(df['Column Name'] == column_name, value[0], df['Classification Details'])
#PII

print('PII ends: {0}'.format(str(datetime.now())))

df['run_date']=datetime.today().strftime('%Y%m%d')

# write .csv file to s3 location
wr.s3.to_csv(df, path=f'{output_location}{output_file_name}', index=False)

job.commit()