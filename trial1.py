#!pip install pygithub google-cloud-secret-manager flatten_json
# Install the module and import it :
#!pip install google-cloud-secret-manager
import requests
from getpass import getpass
import pandas as pd
import json
import flatten_json as fj
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from datetime import datetime,timedelta
import calendar


user_name = os.environ['user_name']
github_token = os.environ['github_token']

#creating a authenticated session
gh_session = requests.Session()
gh_session.auth = (user_name, github_token)

clone_count_df_appended=pd.DataFrame()
view_count_df_appended=pd.DataFrame()
top_referrers_df_appended=pd.DataFrame()
top_paths_df_appended=pd.DataFrame()
star_gazers_df_appended=pd.DataFrame()
subscribers_df_appended=pd.DataFrame()
contributor_commit_activity_df_appended=pd.DataFrame()
list_repository_languages_df_appended=pd.DataFrame()
forks_df_appended=pd.DataFrame()


#repos
#catches everything
repos_get_request= 'https://api.github.com/user/repos'
repos_get_request_json = gh_session.get(repos_get_request).json()
repos_df=pd.DataFrame.from_dict(repos_get_request_json)
repos_list=repos_df['name'].tolist()
repos_list

for repo in repos_list:
  #Clones_traffic
  for i in range(0,50):
    try:
      #repo='IDC-WebApp'
      #get request for clone traffic
      #documentation:https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-repository-clones
      clone_traffic_get_request= 'https://api.github.com/repos/ImagingDataCommons/'+repo+'/traffic/clones'
      #converting api response to json using authenticated session
      try:
        clone_traffic_get_request_json = gh_session.get(clone_traffic_get_request).json()
        print (str(datetime.now())+' '+repo+' ' +'authentication successful while requesting clone traffic')
      except:
        print(str(datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ') 
      #converting json to pandas dataframe
      clone_count_df=pd.DataFrame()
      clone_count_df=pd.json_normalize(clone_traffic_get_request_json, record_path =['clones'])
      #adding a column to indicate the repo_name,adding timestamp, and renaming columns, converting timestamp to datetime64
      clone_count_df['repo']=repo
      clone_count_df['timestamp_data_pulled'] = pd.to_datetime('today')
      clone_count_df=clone_count_df.rename(columns={'count':'clone_count', 'uniques':'unique_clone_count'})
      clone_count_df['timestamp']= pd.to_datetime(clone_count_df['timestamp']).dt.tz_localize(None)
      #getting clone counts only from the day before
      #clone_count_df=clone_count_df[clone_count_df['timestamp']==(datetime.now()- timedelta(days=1)).strftime("%Y-%m-%d")]
      print(str(datetime.now())+' '+repo+' ' +'successfully retreived clone counts into clone_count_df')
      clone_count_df_appended=pd.concat([clone_count_df_appended,clone_count_df])
      break
    except:
      print(str(datetime.now())+' '+repo+' ' +'attempt to retreive clone traffic was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue

  for i in range(0,5):
#    try:
      #setting schema
      clone_count_df_job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField("timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
                                                  bigquery.SchemaField("repo", bigquery.enums.SqlTypeNames.STRING),
                                                  bigquery.SchemaField("clone_count", bigquery.enums.SqlTypeNames.INT64),
                                                  bigquery.SchemaField("unique_clone_count", bigquery.enums.SqlTypeNames.INT64),
                                                  bigquery.SchemaField("timestamp_data_pulled", bigquery.enums.SqlTypeNames.TIMESTAMP),
                                                  ])
      #loading into bq
      clone_count_df_job = client.load_table_from_dataframe(clone_count_df_appended, "idc-external-025.logs.gh_clone_count", job_config=clone_count_df_job_config)
      print('successfully loaded data from clone_count_df_appended dataframe to bigquery')
#      break
#    except:
 #     print('loading data from clone_count_df_appended dataframe to bigquery was unsuccessful/n')
 #     print('retrying to load into bigquery')
  #    continue
