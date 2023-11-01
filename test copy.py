# storage ='s3'
# source_bucket ='iris-etl-framework'
# object_summary = 'data/raw/file_ingestion_source/CustomerData_20220330.csv'

# file_path= f"{storage}://{source_bucket}/{object_summary}"
# print('file_path: ', file_path)
# file = (file_path.split("data/raw/file_ingestion_source/",1)[1])
# print(file)
# # print('file:,' file)


myStr='abc.xls'

myList=['xls','xlsx']


if any(myStr.endswith(s) for s in myList):
    print("Success")