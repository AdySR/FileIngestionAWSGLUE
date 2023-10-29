import pandas as pd
import xlrd
import openpyxl

df= pd.read_excel(r'C:\programs\pyprojects\FileIngestionAWS\file_ingestion_file_source\file_example_XLSX_100.xlsx')


print(df)


# df =pd.read_json(r'C:\programs\pyprojects\FileIngestionAWS\file_ingestion_file_source\sample1.json', typ='series')
# df1 =pd.read_json(r'C:\programs\pyprojects\FileIngestionAWS\file_ingestion_file_source\sample2.json', typ='series')
# df2 =pd.read_json(r'C:\programs\pyprojects\FileIngestionAWS\file_ingestion_file_source\sample4.json', typ='series')

# print(df, '\n\n\n\n\n')
# print(df1, '\n\n\n\n\n')
# print(df2, '\n\n\n\n\n')