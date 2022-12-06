#!pip install pygithub google-cloud-secret-manager flatten_json
# Install the module and import it :
#!pip install google-cloud-secret-manager
import os
import requests
#from getpass import getpass
import pandas as pd
import json
import datetime
import flatten_json as fj


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
  try:
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
    break
  except:
    print('loading data from clone_count_df_appended dataframe to bigquery was unsuccessful/n')
    print('retrying to load into bigquery')
    continue

#Views_traffic
for i in range(0,50):
  try:
    #repo='IDC-WebApp'
    #get request for views traffic
    #documentation:https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-page-views
    view_traffic_get_request='https://api.github.com/repos/ImagingDataCommons/'+repo+'/traffic/views'
    #converting api response to json using authenticated session
    try:
      views_traffic_json= gh_session.get(view_traffic_get_request).json()
      print (str(datetime.now())+' '+repo+' ' +'authentication successful while requesting clone traffic')
    except:
      print(str(datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ') 
    #converting json to pandas dataframe
    view_count_df=pd.DataFrame()  
    view_count_df=pd.json_normalize(views_traffic_json, record_path =['views'])
    #adding a column to indicate the repo_name, timestamp, and renaming columns
    view_count_df['repo']=repo
    view_count_df['timestamp_data_pulled'] = pd.to_datetime('today')
    view_count_df=view_count_df.rename(columns={'count':'views_count', 'uniques':'unique_view_count'})
    view_count_df['timestamp']= pd.to_datetime(view_count_df['timestamp']).dt.tz_localize(None)
    #getting view counts only from the day before
    #view_count_df=view_count_df[view_count_df['timestamp']==(datetime.now()- timedelta(days=1)).strftime("%Y-%m-%d")]
    view_count_df_appended=pd.concat([view_count_df_appended,view_count_df])
    print(str(datetime.now())+' '+repo+' ' +'successfully retreived view counts into view_count_df')
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
    view_count_df_job = client.load_table_from_dataframe(view_count_df_appended, "idc-external-025.logs.gh_views_traffic", job_config=view_count_df_job_config)
    print('successfully loaded data from view_count_df_appended dataframe to bigquery')
    break
  except:
    print('loading data from view_count_df_appended dataframe to bigquery was unsuccessful/n')
    print('retrying to load into bigquery')
    continue


#referrers
for i in range(0,50):
  try:
    #repo='IDC-WebApp'
    #get request for top 10 referrers over the last 14 days.
    #documentation: https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-top-referral-sources
    top_referrers_get_request='https://api.github.com/repos/ImagingDataCommons/'+repo+'/traffic/popular/referrers'
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
    top_referrers_df=top_referrers_df.rename(columns={'count':'referrer_count_all', 'uniques':'referrer_count_unique'})
    top_referrers_df_appended=pd.concat([top_referrers_df_appended,top_referrers_df])
    print(str(datetime.now())+' '+repo+' ' +'successfully retreived top 10 referrers in the last 14 days into top_referrers_df')
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
    top_referrers_df_job = client.load_table_from_dataframe(top_referrers_df_appended, "idc-external-025.logs.gh_top_referrers", job_config=top_referrers_df_job_config)
    print('successfully loaded data from top_referrers_df_appended dataframe to bigquery')
    break
  except:
    print('loading data from top_referrers_df_appended dataframe to bigquery was unsuccessful/n')
    print('retrying to load into bigquery')
    continue


#paths
for i in range(0,50):
  try:
    #repo='IDC-WebApp'
    #get request for popular paths over the last 14 days.
    #documentation: https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-top-referral-paths
    top_paths_get_request='https://api.github.com/repos/ImagingDataCommons/'+repo+'/traffic/popular/paths'
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
    top_paths_df=top_paths_df.rename(columns={'count':'paths_count_all', 'uniques':'paths_count_unique'})
    top_paths_df_appended=pd.concat([top_paths_df_appended,top_paths_df])
    print(str(datetime.now())+' '+repo+' ' +'successfully retreived top 10 paths in the last 14 days into top_paths_df')
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
    top_paths_df_job = client.load_table_from_dataframe(top_paths_df_appended, "idc-external-025.logs.gh_top_paths", job_config=top_paths_df_job_config)
    print('successfully loaded data from top_paths_df_appended dataframe to bigquery')
    break
  except:
    print('loading data from top_paths_df_appended dataframe to bigquery was unsuccessful/n')
    print('retrying to load into bigquery')
    continue

#stargazers
for i in range(0,50):
  try:
    #repo='IDC-WebApp'
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
    star_gazers_df_job = client.load_table_from_dataframe(star_gazers_df_appended, "idc-external-025.logs.gh_star_gazers") 
    print('successfully loaded data from star_gazers_df_appended dataframe to bigquery')
    break
  except:
    print('loading data from star_gazers_df_appended dataframe to bigquery was unsuccessful/n')
    print('retrying to load into bigquery')
    continue

#subscribers
for i in range(0,50):
  try:
    #repo='IDC-WebApp'
    #get request for list of subscribers of the repository
    #documentation: https://docs.github.com/en/rest/activity/watching?apiVersion=2022-11-28#list-watchers
    subscribers_get_request='https://api.github.com/repos/ImagingDataCommons/'+repo+'/subscribers'
    #converting api response to json using authenticated session
    try:
      subscribers_json = gh_session.get(subscribers_get_request).json()
      print(str(datetime.now())+' '+repo+' ' +'authentication successful while requesting list of subscribersof the repository')
    except:
      print(str(datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
    #converting json to pandas dataframe
    subscribers_df=pd.DataFrame()  
    subscribers_df= pd.json_normalize(subscribers_json)
    #adding a column to indicate the repo_name
    subscribers_df['repo']=repo
    subscribers_df['timestamp_data_pulled'] = pd.to_datetime('today')
    #selecting only a few columns
    subscribers_df=subscribers_df[['login', 'html_url','type', 'site_admin', 'repo','timestamp_data_pulled']]
    subscribers_df_appended=pd.concat([subscribers_df_appended,subscribers_df])
    print(str(datetime.now())+' '+repo+' ' +'successfully retreived  list of subscribers into subscribers_df')
    break
  except:
    print(str(datetime.now())+' '+repo+' ' +'attempt to retreive subscribers was unsuccessful, check for errors while converting json response to dataframe/n') 
    print('retrying')
    continue

for i in range(0,5):
  try:
    #not setting schema as there are many columns
    #loading into bq
    subscribers_df_job = client.load_table_from_dataframe(subscribers_df_appended, "idc-external-025.logs.gh_subscribers") 
    print('successfully loaded data from subscribers_df_appended dataframe to bigquery')
    break
  except:
    print('loading data from subscribers_df_appended dataframe to bigquery was unsuccessful/n')
    print('retrying to load into bigquery')
    continue

#contributor_commit_activity
def findDay(date):
    x = datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ').weekday()
    return (calendar.day_name[x])
now=datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

if findDay(now)== 'Monday':
  latest_week_sunday=(datetime.now()- timedelta(days=1)).strftime("%Y-%m-%d")

  for i in range(0,50):
    try:
      #repo='IDC-WebApp'
      #get request for the total number of commits authored by the contributor (the response includes a Weekly Hash (weeks array)) in the repository
      #documentation:https://docs.github.com/en/rest/metrics/statistics?apiVersion=2022-11-28#get-all-contributor-commit-activity
      contributor_commit_activity_get_request= 'https://api.github.com/repos/ImagingDataCommons/'+repo+'/stats/contributors'
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
      contributor_commit_activity_df['timestamp_data_pulled'] = pd.to_datetime('today')
      contributor_commit_activity_df=contributor_commit_activity_df[['w', 'a', 'd', 'c', 'login', 'html_url', 'type', 'site_admin','timestamp_data_pulled']]
      contributor_commit_activity_df=contributor_commit_activity_df[contributor_commit_activity_df['w']==latest_week_sunday]
      contributor_commit_activity_df_appended=pd.concat([contributor_commit_activity_df_appended,contributor_commit_activity_df])
      print(str(datetime.now())+' '+repo+' ' +'successfully retreived all contributor commit activity into contributor_commit_activity_df')
      break 
    except:
      print(str(datetime.now())+' '+repo+' ' +'attempt to retreive all contributor commit activity was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue

  for i in range(0,5):
    try:
      #not setting schema as there are many columns
      #commits_df_job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField("days", bigquery.enums.SqlTypeNames.STRING), ])
                                                
      #loading into bq
      contributor_commit_activity_df_job = client.load_table_from_dataframe(contributor_commit_activity_df_appended, "idc-external-025.logs.gh_contributor_commit_activity") 
      print('successfully loaded data from contributor_commit_activity_df_appended dataframe to bigquery')
      break
    except:
      print('loading data from contributor_commit_activity_df_appended dataframe to bigquery was unsuccessful/n')
      print('retrying to load into bigquery')
      continue

#list_repository_languages
for i in range(0,50):
  try:
    #repo='IDC-WebApp'
    #get request for a list of languages for the specified repository. The value shown for each language is the number of bytes of code written in that language
    #documentation: https://docs.github.com/en/rest/repos/repos?apiVersion=2022-11-28#list-repository-languages
    list_repository_languages= 'https://api.github.com/repos/ImagingDataCommons/'+repo+'/languages'
    #converting api response to json using authenticated session
    try:
      list_repository_languages_json = gh_session.get(list_repository_languages).json()
      print(str(datetime.now())+' '+repo+' ' +'authentication successful while requesting list of languages for the specified repository')
    except:
      print(str(datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
    #converting json to pandas dataframe
    list_repository_languages_df=pd.DataFrame()  
    list_repository_languages_df=pd.json_normalize(list_repository_languages_json)
    #adding a column to indicate the repo_name 
    list_repository_languages_df['repo']=repo
    #adding timestamp of when the data was pulled
    list_repository_languages_df['timestamp_data_pulled'] = pd.to_datetime('today')
    list_repository_languages_df_appended=pd.concat([list_repository_languages_df_appended, list_repository_languages_df])
    print(str(datetime.now())+' '+repo+' ' +'successfully retreived list of repository languages into list_repository_languages_df')
    break 
  except:
    print(str(datetime.now())+' '+repo+' ' +'attempt to retreive a list of repository languages was unsuccessful, check for errors while converting json response to dataframe/n')
    print('retrying')
    continue


for i in range(0,5):
  try:
    #not setting schema as there are many columns
    #commits_df_job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField("days", bigquery.enums.SqlTypeNames.STRING), ])
                                               
    #loading into bq
    list_repository_languages_df_job = client.load_table_from_dataframe(list_repository_languages_df_appended, "idc-external-025.logs.gh_repository_languages") 
    print('successfully loaded data from list_repository_languages_df_appended dataframe to bigquery')
    break
  except:
    print('loading data from list_repository_languages_df_appended dataframe to bigquery was unsuccessful/n')
    print('retrying to load into bigquery')
    continue

#forks
for i in range(0,50):
  try:
    #repo='IDC-WebApp'
    #get request for list of the forks of the repository. lists only the forks originated from the repository and does not list complete fork history. 
    #i.e the current repository may itself be a fork of some other repository
    #documentation: https://docs.github.com/en/rest/repos/forks?apiVersion=2022-11-28#list-forks
    forks= 'https://api.github.com/repos/ImagingDataCommons/'+repo+'/forks'
    #converting api response to json using authenticated session
    try:
      forks_json = gh_session.get(forks).json()
      print(str(datetime.now())+' '+repo+' ' +'authentication successful while requesting list of languages for the specified repository')
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
    forks_df.columns = forks_df.columns.str.replace(r".", "_", regex=True)
    forks_df=forks_df[['full_name','private','html_url','fork','created_at','owner_login','owner_html_url','repo','timestamp_data_pulled']]
    forks_df_appended=pd.concat([forks_df_appended,forks_df])
    print(str(datetime.now())+' '+repo+' ' +'successfully retreived list of forks into forks_df')
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
    forks_df_job = client.load_table_from_dataframe(forks_df_appended, "idc-external-025.logs.gh_forks") 
    print('successfully loaded data from forks_df_appended dataframe to bigquery')
    break
  except:
    print('loading data from forks_df_appended dataframe to bigquery was unsuccessful/n')
    print('retrying to load into bigquery')
    continue




