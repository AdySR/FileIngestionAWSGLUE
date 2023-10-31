filepath= r's3://iris-etl-framework/data/raw/file_ingestion_source/CustomerData_20220330.csv'

print(filepath.split("data/raw/file_ingestion_source/",1)[1])

f"{storage}://{source_bucket}/{object_summary.key}"

print(filepath[filepath.index('/') + len('/'):])
