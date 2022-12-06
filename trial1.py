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
  #stargazers
  for i in range(0,50):
    #try:
      repo='IDC-WebApp'
      #get request for list of people that have starred the repository
      #documentation:https://docs.github.com/en/rest/activity/starring?apiVersion=2022-11-28#list-stargazers
      star_gazers_get_request='https://api.github.com/repos/ImagingDataCommons/'+repo+'/stargazers'
      #converting api response to json using authenticated session
      try:
        star_gazers_json = gh_session.get(star_gazers_get_request).json()
        print (str(datetime.now())+' '+repo+' ' +'authentication successful while requesting for list of people that have starred the repository')
      except:
        print(str(datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe
      star_gazers_df=pd.DataFrame() 
      star_gazers_df= pd.json_normalize(star_gazers_json)
      #adding a column to indicate the repo_name, and a timestamp
      star_gazers_df['repo']=repo
      star_gazers_df['timestamp_data_pulled'] = pd.to_datetime('today')
      #selecting only a few columns
      star_gazers_df=star_gazers_df[['login', 'html_url','type', 'site_admin', 'repo','timestamp_data_pulled']]
      star_gazers_df_appended=pd.concat([star_gazers_df_appended,star_gazers_df]) 
      print(str(datetime.now())+' '+repo+' ' +'successfully retreived the list of stargazers into star_gazers_df')
  #    break
  #   except:
  #     print(str(datetime.now())+' '+repo+' ' +'attempt to retreive stargazers was unsuccessful, check for errors while converting json response to dataframe/n') 
  #     print('retrying')
  #     continue

  # for i in range(0,5):
  #   try:
  #     #not setting schema as there are many columns
  #     #star_gazers_df_job_config = []
  #     #loading into bq
  #     star_gazers_df_job = client.load_table_from_dataframe(star_gazers_df_appended, "idc-external-025.logs.gh_star_gazers") 
  #     print('successfully loaded data from star_gazers_df_appended dataframe to bigquery')
  #     break
  #   except:
  #     print('loading data from star_gazers_df_appended dataframe to bigquery was unsuccessful/n')
  #     print('retrying to load into bigquery')
  #     continue
