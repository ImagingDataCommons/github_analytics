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
from time import sleep


user_name = os.environ['user_name']
github_token = os.environ['github_token']

#creating a authenticated session
gh_session = requests.Session()
gh_session.auth = (user_name, github_token)
client = bigquery.Client()

clone_count_df_appended=pd.DataFrame()
view_count_df_appended=pd.DataFrame()
top_referrers_df_appended=pd.DataFrame()
top_paths_df_appended=pd.DataFrame()
star_gazers_df_appended=pd.DataFrame()
subscribers_df_appended=pd.DataFrame()
contributor_commit_activity_df_appended=pd.DataFrame()
list_repository_languages_df_appended=pd.DataFrame()
forks_df_appended=pd.DataFrame()

def findDay(date):
    x = datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ').weekday()
    return (calendar.day_name[x])
now=datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")


#repos
#catches everything
public_repos_get_request= 'https://api.github.com/orgs/ImagingDataCommons/repos?type=public'
public_repos_get_request_json = gh_session.get(public_repos_get_request).json()
public_repos_df=pd.DataFrame.from_dict(public_repos_get_request_json)
public_repos_list=public_repos_df['name'].tolist()

private_repos_get_request= 'https://api.github.com/orgs/ImagingDataCommons/repos?type=private'
private_repos_get_request_json = gh_session.get(private_repos_get_request).json()
private_repos_df=pd.DataFrame.from_dict(private_repos_get_request_json)
private_repos_list=private_repos_df['name'].tolist()

repos_list=public_repos_list+private_repos_list

#adding repos from qiicr organization from 8/6/23
repos_list=repos_list+['QuantitativeReporting','dcmqi','TCIABrowser']



for repo in repos_list:
#clones_traffic  
  for i in range(0,50):
    try:
      #repo='IDC-WebApp'
      #get request for clone traffic
      #documentation:https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-repository-clones
        if repo in ['TCIABrowser', 'dcmqi', 'QuantitativeReporting']:
            organization = 'qiicr'
        else:
            organization = 'ImagingDataCommons'
      clone_traffic_get_request= f'https://api.github.com/repos/{organization}/{repo}/traffic/clones'
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
      if 'count' in clone_count_df.columns:
        clone_count_df=clone_count_df.rename(columns={'count':'clone_count', 'uniques':'unique_clone_count'})
        clone_count_df['timestamp']= pd.to_datetime(clone_count_df['timestamp']).dt.tz_localize(None)
        #getting clone counts only from the day before
        clone_count_df=clone_count_df[clone_count_df['timestamp']==(datetime.now()- timedelta(days=1)).strftime("%Y-%m-%d")]
        print(str(datetime.now())+' '+repo+' ' +'successfully retreived clone counts into clone_count_df')
        clone_count_df_appended=pd.concat([clone_count_df_appended,clone_count_df])
        break
      else:
        print('skipped converting api response to dataframe')
        break
      break     
    except:
      print(str(datetime.now())+' '+repo+' ' +'attempt to retreive clone traffic was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue

for i in range(0,5):
  try:
     #setting schema
      clone_count_df_job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField("timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
                                                 bigquery.SchemaField("repo", bigquery.enums.SqlTypeNames.STRING),
                                                 bigquery.SchemaField("clone_count", bigquery.enums.SqlTypeNames.INT64),
                                                 bigquery.SchemaField("unique_clone_count", bigquery.enums.SqlTypeNames.INT64),
                                                 bigquery.SchemaField("timestamp_data_pulled", bigquery.enums.SqlTypeNames.TIMESTAMP),
                                                 ])
     #loading into bq
      clone_count_df_job = client.load_table_from_dataframe(clone_count_df_appended, "idc-external-025.logs.gh_clone_count_test", job_config=clone_count_df_job_config)
      print('successfully loaded data from clone_count_df_appended dataframe to bigquery')
      break
  except:
      print('loading data from clone_count_df_appended dataframe to bigquery was unsuccessful/n')
      print('retrying to load into bigquery')
      continue

#Views_traffic
for repo in repos_list:
  for i in range(0,50):
    try:
      #repo='IDC-WebApp'
      #get request for views traffic
      #documentation:https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-page-views
      if repo in ['TCIABrowser', 'dcmqi', 'QuantitativeReporting']:
        organization = 'qiicr'
      else:
        organization = 'ImagingDataCommons'
      view_traffic_get_request=f'https://api.github.com/repos/{organization}/{repo}/traffic/views'
      #converting api response to json using authenticated session
      try:
        views_traffic_json= gh_session.get(view_traffic_get_request).json()
        print (str(datetime.now())+' '+repo+' ' +'authentication successful while requesting views traffic')
      except:
        print(str(datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ') 
      #converting json to pandas dataframe
      view_count_df=pd.DataFrame()  
      view_count_df=pd.json_normalize(views_traffic_json, record_path =['views'])
      #adding a column to indicate the repo_name, timestamp, and renaming columns
      view_count_df['repo']=repo
      view_count_df['timestamp_data_pulled'] = pd.to_datetime('today')
      if 'count' in view_count_df.columns:
        view_count_df=view_count_df.rename(columns={'count':'views_count', 'uniques':'unique_view_count'})
        view_count_df['timestamp']= pd.to_datetime(view_count_df['timestamp']).dt.tz_localize(None)
        #getting view counts only from the day before
        view_count_df=view_count_df[view_count_df['timestamp']==(datetime.now()- timedelta(days=1)).strftime("%Y-%m-%d")]
        view_count_df_appended=pd.concat([view_count_df_appended,view_count_df])
        print(str(datetime.now())+' '+repo+' ' +'successfully retreived view counts into view_count_df')
        break
      else:
          print('skipped converting api response to dataframe')
          break
      break
    except:
      print(str(datetime.now())+' '+repo+' ' +'attempt to retreive views traffic was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue
for i in range(0,5):
  try:
    #setting schema
    view_count_df_job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField("timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
                                                bigquery.SchemaField("views_count", bigquery.enums.SqlTypeNames.INT64),
                                                bigquery.SchemaField("unique_view_count", bigquery.enums.SqlTypeNames.INT64),
                                                bigquery.SchemaField("repo", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("timestamp_data_pulled", bigquery.enums.SqlTypeNames.TIMESTAMP),
                                                ])
    #loading into bq
    view_count_df_job = client.load_table_from_dataframe(view_count_df_appended, "idc-external-025.logs.gh_views_traffic_test", job_config=view_count_df_job_config)
    print('successfully loaded data from view_count_df_appended dataframe to bigquery')
    break
  except:
    print('loading data from view_count_df_appended dataframe to bigquery was unsuccessful/n')
    print('retrying to load into bigquery')
    continue

      
#referrers
for repo in repos_list:
  for i in range(0,50):
    try:
      #repo='IDC-WebApp'
      #get request for top 10 referrers over the last 14 days.
      #documentation: https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-top-referral-sources
      if repo in ['TCIABrowser', 'dcmqi', 'QuantitativeReporting']:
          organization = 'qiicr'
      else:
          organization = 'ImagingDataCommons'     

      top_referrers_get_request=f'https://api.github.com/repos/{organization}/{repo}/traffic/popular/referrers'
      #converting api response to json using authenticated session
      try:
        top_referrers_json = gh_session.get(top_referrers_get_request).json()
        print(str(datetime.now())+' '+repo+' ' +'authentication successful while requesting top 10 referrers in the last 14 days')
      except:
        print(str(datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe
      top_referrers_df=pd.DataFrame() 
      top_referrers_df= pd.json_normalize(top_referrers_json)
      #adding a column to indicate the repo_name, timestamp, and renaming columns
      top_referrers_df['repo']=repo
      top_referrers_df['timestamp_data_pulled'] = pd.to_datetime('today')
      if 'count' in top_referrers_df.columns:
        top_referrers_df=top_referrers_df.rename(columns={'count':'referrer_count_all', 'uniques':'referrer_count_unique'})
        top_referrers_df_appended=pd.concat([top_referrers_df_appended,top_referrers_df])
        print(str(datetime.now())+' '+repo+' ' +'successfully retreived top 10 referrers in the last 14 days into top_referrers_df')
        break
      else:
        print('skipped converting api response to dataframe')
        break
      break      
    except:
      print(str(datetime.now())+' '+repo+' ' +'attempt to retreive top 10 referrers in the last 14 days was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue
for i in range(0,5):
  try:
    #setting schema
    top_referrers_df_job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField("referrer", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("referrer_count_all", bigquery.enums.SqlTypeNames.INT64),
                                                bigquery.SchemaField("referrer_count_unique", bigquery.enums.SqlTypeNames.INT64),
                                                bigquery.SchemaField("repo", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("timestamp_data_pulled", bigquery.enums.SqlTypeNames.TIMESTAMP),
                                                ])
    #loading into bq
    top_referrers_df_job = client.load_table_from_dataframe(top_referrers_df_appended, "idc-external-025.logs.gh_top_referrers_test", job_config=top_referrers_df_job_config)
    print('successfully loaded data from top_referrers_df_appended dataframe to bigquery')
    break
  except:
    print('loading data from top_referrers_df_appended dataframe to bigquery was unsuccessful/n')
    print('retrying to load into bigquery')
    continue


#paths
for repo in repos_list:
  for i in range(0,50):
    try:
      #repo='IDC-WebApp'
      #get request for popular paths over the last 14 days.
      #documentation: https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-top-referral-paths
      if repo in ['TCIABrowser', 'dcmqi', 'QuantitativeReporting']:
          organization = 'qiicr'
      else:
          organization = 'ImagingDataCommons'      
      top_paths_get_request=f'https://api.github.com/repos/{organization}/{repo}/traffic/popular/paths'
      #converting api response to json using authenticated session
      try:
        top_paths_json = gh_session.get(top_paths_get_request).json()
        print(str(datetime.now())+' '+repo+' ' +'authentication successful while requesting top 10 paths in the last 14 days')
      except:
        print(str(datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe
      top_paths_df=pd.DataFrame() 
      top_paths_df= pd.json_normalize(top_paths_json)
      #adding a column to indicate the repo_name,timestamp, and renaming columns
      top_paths_df['repo']=repo
      top_paths_df['timestamp_data_pulled'] = pd.to_datetime('today')
      if 'count' in top_paths_df.columns:
        top_paths_df=top_paths_df.rename(columns={'count':'paths_count_all', 'uniques':'paths_count_unique'})
        top_paths_df_appended=pd.concat([top_paths_df_appended,top_paths_df])
        print(str(datetime.now())+' '+repo+' ' +'successfully retreived top 10 paths in the last 14 days into top_paths_df')
        break
      else:
        print('skipped converting api response to dataframe')
        break
      break          
    except:
      print(str(datetime.now())+' '+repo+' ' +'attempt to retreive top 10 paths in the last 14 days was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue
for i in range(0,5):
  try:
    #setting schema
    top_paths_df_job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField("path", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("paths_count_all", bigquery.enums.SqlTypeNames.INT64),
                                                bigquery.SchemaField("paths_count_unique", bigquery.enums.SqlTypeNames.INT64),
                                                bigquery.SchemaField("repo", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("timestamp_data_pulled", bigquery.enums.SqlTypeNames.TIMESTAMP),
                                                ])
    #loading into bq
    top_paths_df_job = client.load_table_from_dataframe(top_paths_df_appended, "idc-external-025.logs.gh_top_paths_test", job_config=top_paths_df_job_config)
    print('successfully loaded data from top_paths_df_appended dataframe to bigquery')
    break
  except:
    print('loading data from top_paths_df_appended dataframe to bigquery was unsuccessful/n')
    print('retrying to load into bigquery')
    continue      

#stargazers
yesterday=(datetime.now()- timedelta(days=1)).strftime("%Y-%m-%d")
today=(datetime.now()).strftime("%Y-%m-%d")

for repo in repos_list:
  #stargazers
  for i in range(0,50):
    try:
      #repo='ai_medima_misc '
      #get request for list of people that have starred the repository
      #documentation:https://docs.github.com/en/rest/activity/starring?apiVersion=2022-11-28#list-stargazers
      if repo in ['TCIABrowser', 'dcmqi', 'QuantitativeReporting']:
          organization = 'qiicr'
      else:
          organization = 'ImagingDataCommons'      
      star_gazers_get_request=f'https://api.github.com/repos/{organization}/{repo}/stargazers'
      #converting api response to json using authenticated session
      try:
        star_gazers_json = gh_session.get(star_gazers_get_request,headers ={'accept':'application/vnd.github.v3.star+json'}).json()
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
      if 'user.login' in star_gazers_df.columns:
        star_gazers_df=star_gazers_df[['starred_at','user.login', 'user.html_url','user.type', 'user.site_admin', 'repo','timestamp_data_pulled']]
        star_gazers_df.columns = star_gazers_df.columns.str.replace(r".", "_", regex=True)
        star_gazers_df=star_gazers_df[(star_gazers_df['starred_at']>yesterday)& (star_gazers_df['starred_at']<today) ]
        star_gazers_df_appended=pd.concat([star_gazers_df_appended,star_gazers_df]) 
        print(str(datetime.now())+' '+repo+' ' +'successfully retreived the list of stargazers into star_gazers_df')
      else:
        print('skipped converting api response to dataframe')
        break
      break
    except:
      print(str(datetime.now())+' '+repo+' ' +'attempt to retreive stargazers was unsuccessful, check for errors while converting json response to dataframe/n') 
      print('retrying')
      continue

for i in range(0,5):
  try:
    #not setting schema as there are many columns
    #star_gazers_df_job_config = []
    #loading into bq
    star_gazers_df_job = client.load_table_from_dataframe(star_gazers_df_appended, "idc-external-025.logs.gh_star_gazers_test") 
    print('successfully loaded data from star_gazers_df_appended dataframe to bigquery')
    break
  except:
    print('loading data from star_gazers_df_appended dataframe to bigquery was unsuccessful/n')
    print('retrying to load into bigquery')
    continue


#contributor_commit_activity
if findDay(now)== 'Monday':
  for repo in repos_list:

    latest_week_sunday=(datetime.now()- timedelta(days=1)).strftime("%Y-%m-%d")

    for i in range(0,50):
      try:
        #repo='IDC-WebApp'
        #get request for the total number of commits authored by the contributor (the response includes a Weekly Hash (weeks array)) in the repository
        #documentation:https://docs.github.com/en/rest/metrics/statistics?apiVersion=2022-11-28#get-all-contributor-commit-activity
        if repo in ['TCIABrowser', 'dcmqi', 'QuantitativeReporting']:
            organization = 'qiicr'
        else:
            organization = 'ImagingDataCommons'        
        contributor_commit_activity_get_request= f'https://api.github.com/repos/{organization}/{repo}/stats/contributors'
        #converting api response to json using authenticated session
        try:
          contributor_commit_activity_json = gh_session.get(contributor_commit_activity_get_request).json()
          print(str(datetime.now())+' '+repo+' ' +'authentication successful while requesting the total number of commits authored by the contributor')
        except:
          print(str(datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
        #converting json to pandas dataframe and flattening it
        contributor_commit_activity_df=pd.DataFrame() 
        #adding a column to indicate the repo_name and renaming columns
        contributor_commit_activity_df=pd.DataFrame.from_dict(contributor_commit_activity_json)
        contributor_commit_activity_df=contributor_commit_activity_df.explode(['weeks'])
        contributor_commit_activity_df=pd.concat([contributor_commit_activity_df.drop(['weeks'], axis=1), contributor_commit_activity_df['weeks'].apply(pd.Series)], axis=1)
        contributor_commit_activity_df=pd.concat([contributor_commit_activity_df.drop(['author'], axis=1), contributor_commit_activity_df['author'].apply(pd.Series)], axis=1)
        #converting all columns with unix timestamps to datetime after getting a list of all columns ending with _w
        #cols=[col for col in contributor_commit_activity_df.columns if 'w' in col]
        contributor_commit_activity_df['w']=contributor_commit_activity_df['w'].apply(pd.to_datetime, unit = 's')
        #adding timestamp of when the data was pulled
        contributor_commit_activity_df['repo']=repo
        contributor_commit_activity_df['timestamp_data_pulled'] = pd.to_datetime('today')
        if 'login' in contributor_commit_activity_df.columns:
          contributor_commit_activity_df=contributor_commit_activity_df[['w', 'a', 'd', 'c', 'login', 'html_url', 'type', 'site_admin','timestamp_data_pulled','repo']]
          contributor_commit_activity_df=contributor_commit_activity_df[contributor_commit_activity_df['w']==latest_week_sunday]
          contributor_commit_activity_df_appended=pd.concat([contributor_commit_activity_df_appended,contributor_commit_activity_df])
          print(str(datetime.now())+' '+repo+' ' +'successfully retreived all contributor commit activity into contributor_commit_activity_df')
        else:
          print('skipped converting api response to dataframe')
          break
        break    
      except:
        print(str(datetime.now())+' '+repo+' ' +'attempt to retreive all contributor commit activity was unsuccessful, check for errors while converting json response to dataframe/n')
        print('retrying')
        print('waiting 15 seconds before recalling API')
        sleep(15)  
        continue

  for i in range(0,5):
    try:
      #not setting schema as there are many columns
      #commits_df_job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField("days", bigquery.enums.SqlTypeNames.STRING), ])
                                                
      #loading into bq
      contributor_commit_activity_df_job = client.load_table_from_dataframe(contributor_commit_activity_df_appended, "idc-external-025.logs.gh_contributor_commit_activity_test") 
      print('successfully loaded data from contributor_commit_activity_df_appended dataframe to bigquery')
      break
    except:
      print('loading data from contributor_commit_activity_df_appended dataframe to bigquery was unsuccessful/n') 
      continue


#forks
if findDay(now)== 'Monday':
  for repo in repos_list:
    current_week_monday=(datetime.now()).strftime("%Y-%m-%d")
    last_week_monday=(datetime.now()- timedelta(days=7)).strftime("%Y-%m-%d")
    for i in range(0,50):
      try:
        #repo='IDC-WebApp'
        #get request for list of the forks of the repository. lists only the forks originated from the repository and does not list complete fork history. 
        #i.e the current repository may itself be a fork of some other repository
        #documentation: https://docs.github.com/en/rest/repos/forks?apiVersion=2022-11-28#list-forks
        if repo in ['TCIABrowser', 'dcmqi', 'QuantitativeReporting']:
            organization = 'qiicr'
        else:
            organization = 'ImagingDataCommons'        
        forks= f'https://api.github.com/repos/{organization}/{repo}/forks'
        #converting api response to json using authenticated session
        try:
          forks_json = gh_session.get(forks).json()
          print(str(datetime.now())+' '+repo+' ' +'authentication successful while requesting list of forks for the specified repository')
        except:
          print(str(datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ') 
        #converting json to pandas dataframe
        forks_df=pd.DataFrame() 
        forks_df=pd.json_normalize(forks_json)
        #adding a column to indicate the repo_name 
        forks_df['repo']=repo
        #adding timestamp of when the data was pulled
        forks_df['timestamp_data_pulled'] = pd.to_datetime('today')
        #replacing columns with . to _
        if 'full_name' in forks_df.columns:
          forks_df.columns = forks_df.columns.str.replace(r".", "_", regex=True)
          forks_df=forks_df[['full_name','private','html_url','fork','created_at','owner_login','owner_html_url','repo','timestamp_data_pulled']]
          forks_df=forks_df[(forks_df['created_at']>= last_week_monday) & (forks_df['created_at']< current_week_monday)]
          forks_df_appended=pd.concat([forks_df_appended,forks_df])
          print(str(datetime.now())+' '+repo+' ' +'successfully retreived list of forks into forks_df')
        else:
            print('skipped converting api response to dataframe')
            break
        break     
      except:
        print(str(datetime.now())+' '+repo+' ' +'attempt to retreive a list of forks was unsuccessful, check for errors while converting json response to dataframe/n')
        print('retrying')
        continue

  for i in range(0,5):
    try:
      #not setting schema as there are many columns
      #commits_df_job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField("days", bigquery.enums.SqlTypeNames.STRING), ])
                                                
      #loading into bq
      forks_df_job = client.load_table_from_dataframe(forks_df_appended, "idc-external-025.logs.gh_forks_test") 
      print('successfully loaded data from forks_df_appended dataframe to bigquery')
      break
    except:
      print('loading data from forks_df_appended dataframe to bigquery was unsuccessful/n')
      print('retrying to load into bigquery')
      continue
