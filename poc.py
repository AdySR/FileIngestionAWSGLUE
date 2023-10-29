from datetime import datetime
import awswrangler as wr
import pandas as pd
import ydata_profiling as pp
import json

file_name='organizations_100.csv'

file_name1 = file_name[:-4]

input_location = r'C:\programs\pyprojects\FileIngestionAWS\input_location'
output_location = r'C:\programs\pyprojects\FileIngestionAWS\output_location'


input_df = pd.read_csv(f"{input_location}\\{file_name}")
df_dict=input_df.dtypes.to_dict()


run_date = datetime.today().strftime('%Y%m%d')
profile = pp.ProfileReport(input_df, title="Data Profile Report",  minimal = True)
output_file_name_html = F"{output_location}\\{file_name1}_{run_date}_html_profile.html"
# profile.to_file(output_file_name_html)

output_file_name_csv = f"{file_name1}_{run_date}_csv_profile.csv"

json_obj = json.loads(profile.to_json())
print('profile_type: {0} json_obj: {1}'.format(type(profile),type(json_obj)))
df = pd.DataFrame(json_obj["variables"])
print(df.head(10))

df2= df.transpose
print(df2)