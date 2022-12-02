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


user_name = os.environ['USER_NAME']
github_token = os.environ['API_TOKEN']

#creating a authenticated session
gh_session = requests.Session()
gh_session.auth = (user_name, github_token)

clone_count_df_appended=pd.DataFrame()
view_count_df_appended=pd.DataFrame()
top_referrers_df_appended=pd.DataFrame()
top_paths_df_appended=pd.DataFrame()
star_gazers_df_appended=pd.DataFrame()
subscribers_df_appended=pd.DataFrame()
list_collaborators_df_appended=pd.DataFrame()
file_contents_df_appended=pd.DataFrame()
weekly_commit_activity_additions_deletions_df_appended=pd.DataFrame()
commits_df_appended=pd.DataFrame()
contributor_commit_activity_df_appended=pd.DataFrame()
weekly_commit_count_df_appended=pd.DataFrame()
hourly_commit_count_for_each_day_df_appended=pd.DataFrame()
pull_requests_all_df_appended=pd.DataFrame()
releases_get_request_df_appended=pd.DataFrame()
list_repository_languages_df_appended=pd.DataFrame()
forks_df_appended=pd.DataFrame()
events_df_appended=pd.DataFrame()
branches_df_appended=pd.DataFrame()
issues_df_appended=pd.DataFrame()

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
        print (str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting clone traffic')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ') 
      #converting json to pandas dataframe
      clone_count_df=pd.DataFrame()
      clone_count_df=pd.json_normalize(clone_traffic_get_request_json, record_path =['clones'])
      #adding a column to indicate the repo_name,adding timestamp, and renaming columns
      clone_count_df['repo']=repo
      clone_count_df['timestamp_data_pulled'] = pd.to_datetime('today')
      clone_count_df=clone_count_df.rename(columns={'count':'clone_count', 'uniques':'unique_clone_count'})
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived clone counts into clone_count_df')
      clone_count_df_appended=pd.concat([clone_count_df_appended,clone_count_df])
      break
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive clone traffic was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue
  clone_count_df_appended

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
        print (str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting clone traffic')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ') 
      #converting json to pandas dataframe
      view_count_df=pd.DataFrame()  
      view_count_df=pd.json_normalize(views_traffic_json, record_path =['views'])
      #adding a column to indicate the repo_name, timestamp, and renaming columns
      view_count_df['repo']=repo
      view_count_df['timestamp_data_pulled'] = pd.to_datetime('today')
      view_count_df=view_count_df.rename(columns={'count':'views_count', 'uniques':'unique_view_count'})
      view_count_df_appended=pd.concat([view_count_df_appended,view_count_df])
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived view counts into view_count_df')
      break
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive views traffic was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue
  view_count_df_appended

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
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting top 10 referrers in the last 14 days')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe
      top_referrers_df=pd.DataFrame() 
      top_referrers_df= pd.json_normalize(top_referrers_json)
      #adding a column to indicate the repo_name, timestamp, and renaming columns
      top_referrers_df['repo']=repo
      top_referrers_df['timestamp_data_pulled'] = pd.to_datetime('today')
      top_referrers_df=top_referrers_df.rename(columns={'count':'referrer_count_all', 'uniques':'referrer_count_unique'})
      top_referrers_df_appended=pd.concat([top_referrers_df_appended,top_referrers_df])
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived top 10 referrersin the last 14 days into top_referrers_df')
      break
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive top 10 referrers in the last 14 days was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue
  top_referrers_df_appended

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
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting top 10 paths in the last 14 days')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe
      top_paths_df=pd.DataFrame() 
      top_paths_df= pd.json_normalize(top_paths_json)
      #adding a column to indicate the repo_name,timestamp, and renaming columns
      top_paths_df['repo']=repo
      top_paths_df['timestamp_data_pulled'] = pd.to_datetime('today')
      top_paths_df=top_paths_df.rename(columns={'count':'paths_count_all', 'uniques':'paths_count_unique'})
      top_paths_df_appended=pd.concat([top_paths_df_appended,top_paths_df])
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived top 10 paths in the last 14 days into top_paths_df')
      break
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive top 10 paths in the last 14 days was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue
  top_paths_df_appended

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
        print (str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting for list of people that have starred the repository')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe
      star_gazers_df=pd.DataFrame() 
      star_gazers_df= pd.json_normalize(star_gazers_json)
      #adding a column to indicate the repo_name, and a timestamp
      star_gazers_df['repo']=repo
      star_gazers_df['timestamp_data_pulled'] = pd.to_datetime('today')
      star_gazers_df_appended=pd.concat([star_gazers_df_appended,star_gazers_df]) 
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived the list of stargazers into star_gazers_df')
      break
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive stargazers was unsuccessful, check for errors while converting json response to dataframe/n') 
      print('retrying')
      continue
  star_gazers_df_appended

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
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting list of subscribersof the repository')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe
      subscribers_df=pd.DataFrame()  
      subscribers_df= pd.json_normalize(subscribers_json)
      #adding a column to indicate the repo_name
      subscribers_df['repo']=repo
      subscribers_df['timestamp_data_pulled'] = pd.to_datetime('today')
      subscribers_df_appended=pd.concat([subscribers_df_appended,subscribers_df])
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived  list of subscribers into subscribers_df')
      break
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive subscribers was unsuccessful, check for errors while converting json response to dataframe/n') 
      print('retrying')
      continue
  subscribers_df_appended

  #collaborators
  for i in range(0,50):
    try:
      #repo='IDC-WebApp'
      #get request for list of collaborators of the repository
      #documentation:https://docs.github.com/en/rest/collaborators/collaborators?apiVersion=2022-11-28#list-repository-collaborators
      list_collaborators_get_request= 'https://api.github.com/repos/ImagingDataCommons/'+repo+'/collaborators'
      #converting api response to json using authenticated session
      try:
        list_collaborators_json = gh_session.get(list_collaborators_get_request).json()
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting list of collaborators of the repository')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe
      list_collaborators_df=pd.DataFrame()   
      list_collaborators_df= pd.json_normalize(list_collaborators_json)
      #adding a column to indicate the repo_name, and a timestamp
      list_collaborators_df['repo']=repo
      list_collaborators_df['timestamp_data_pulled'] = pd.to_datetime('today')
      list_collaborators_df_appended=pd.concat([list_collaborators_df_appended,list_collaborators_df])
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived  list of collaborators into subscribers_df')
      break
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive collaborators was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue
  list_collaborators_df_appended

  #file_contents
  for i in range(0,50):
    try:
      #repo='IDC-WebApp'
      #get request for list of file contents of the repository
      #documentation:https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28#get-repository-content
      #Gets the contents of a file or directory in a repository. Specify the file path or directory in :path. If you omit :path, you will receive the contents of the repository's root directory. 
      file_contents_get_request= 'https://api.github.com/repos/ImagingDataCommons/'+repo+'/contents'
      #converting api response to json using authenticated session
      try:
        file_contents_json = gh_session.get(file_contents_get_request).json()
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting the list of file contents of the repository')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe
      file_contents_df=pd.DataFrame()  
      file_contents_df= pd.json_normalize(file_contents_json)
      #adding a column to indicate the repo_name
      file_contents_df['repo']=repo
      file_contents_df['timestamp_data_pulled'] = pd.to_datetime('today')
      file_contents_df_appended=pd.concat([file_contents_df_appended,file_contents_df])
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived file contents of the root directory (default) into file_contents_df')
      break
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive file contents was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue    
  
  file_contents_df_appended

  #weekly_commit_activity_additions_deletions
  for i in range(0,50):
    try:
      #repo='IDC-Common'
      #get request for weekly commit activity additions deletions of the repository
      #documentation: https://docs.github.com/en/rest/metrics/statistics?apiVersion=2022-11-28#get-the-weekly-commit-activity
      #Returns a weekly aggregate of the number of additions and deletions pushed to a repository
      weekly_commit_activity_additions_deletions_get_request= 'https://api.github.com/repos/ImagingDataCommons/'+repo+'/stats/code_frequency'
      #converting api response to json using authenticated session
      try:
        weekly_commit_activity_additions_deletions_json = gh_session.get(weekly_commit_activity_additions_deletions_get_request).json()
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting weekly commit activity additions deletions of the repository')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe
      weekly_commit_activity_additions_deletions_df=pd.DataFrame()  
      weekly_commit_activity_additions_deletions_df= pd.DataFrame.from_dict(weekly_commit_activity_additions_deletions_json)
      #adding a column to indicate the repo_name and renaming columns
      weekly_commit_activity_additions_deletions_df['repo']=repo
      weekly_commit_activity_additions_deletions_df['timestamp_data_pulled'] = pd.to_datetime('today')
      weekly_commit_activity_additions_deletions_df=weekly_commit_activity_additions_deletions_df.rename(columns={0:'timestamp',1:'additions', 2:'deletions'})
      #converting unix time stamp to utc timestamp
      weekly_commit_activity_additions_deletions_df['time']=pd.to_datetime(weekly_commit_activity_additions_deletions_df['timestamp'], unit='s')
      weekly_commit_activity_additions_deletions_df_appended=pd.concat([weekly_commit_activity_additions_deletions_df_appended,weekly_commit_activity_additions_deletions_df])
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived weekly commit activity into weekly_commit_activity_additions_deletions_df')
      break 
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive weekly aggregate of commit activity additions deletions was unsuccessful, check for errors while converting json response to dataframe/n') 
      print('retrying')
      continue
    
  weekly_commit_activity_additions_deletions_df_appended

  #commits
  for i in range(0,50):
    try:
      ##repo='IDC-WebApp'
      #get request for last year of commit activity grouped by week of the repository
      #documentation: https://docs.github.com/en/rest/metrics/statistics?apiVersion=2022-11-28#get-the-last-year-of-commit-activity
      commits_get_request= 'https://api.github.com/repos/ImagingDataCommons/'+repo+'/stats/commit_activity'
      #converting api response to json using authenticated session
      try:
        commits_json = gh_session.get(commits_get_request).json()
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting the last year of commit activity grouped by week of the repository')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ') 
      #converting json to pandas dataframe
      commits_df=pd.DataFrame() 
      #commits_df=pd.DataFrame.from_dict(commits_json)
      commits_df=pd.json_normalize(commits_json)
      #adding a column to indicate the repo_name, timestamp and renaming columns
      commits_df['repo']=repo
      commits_df['timestamp_data_pulled'] = pd.to_datetime('today')
      commits_df=commits_df.rename(columns={'total':'total commits','week':'week_timestamp'})
      #converting unix time stamp to utc timestamp
      commits_df['week_timestamp']=pd.to_datetime(commits_df['week_timestamp'], unit='s')
      commits_df_appended=pd.concat([commits_df_appended,commits_df])
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived last year of commit activity grouped by week into commits_df')
      break 
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive the last year of commit activity was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue
  commits_df_appended

  #contributor_commit_activity
  for i in range(0,50):
    try:
      #repo='IDC-WebApp'
      #get request for the total number of commits authored by the contributor (the response includes a Weekly Hash (weeks array)) in the repository
      #documentation:https://docs.github.com/en/rest/metrics/statistics?apiVersion=2022-11-28#get-all-contributor-commit-activity
      contributor_commit_activity_get_request= 'https://api.github.com/repos/ImagingDataCommons/'+repo+'/stats/contributors'
      #converting api response to json using authenticated session
      try:
        contributor_commit_activity_json = gh_session.get(contributor_commit_activity_get_request).json()
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting the total number of commits authored by the contributor')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe and flattening it
      contributor_commit_activity_df=pd.DataFrame() 
      contributor_commit_activity_df=pd.DataFrame((fj.flatten(d) for d in contributor_commit_activity_json))
      #converting all columns with unix timestamps to datetime after getting a list of all columns ending with _w
      cols=[col for col in contributor_commit_activity_df.columns if '_w' in col]
      contributor_commit_activity_df[cols]=contributor_commit_activity_df[cols].apply(pd.to_datetime, unit = 's')
      #adding timestamp of when the data was pulled
      contributor_commit_activity_df['timestamp_data_pulled'] = pd.to_datetime('today')
      contributor_commit_activity_df_appended=pd.concat([contributor_commit_activity_df_appended,contributor_commit_activity_df])
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived all contributor commit activity into contributor_commit_activity_df')
      break 
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive all contributor commit activity was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue
  contributor_commit_activity_df_appended

  #weekly_commit_count
  for i in range(0,50):
    try:
      #repo='IDC-WebApp'
      #get request for the total commit counts for the owner and total commit counts in all (all is everyone combined, including the owner in the last 52 weeks) of the repository
      #documentation:https://docs.github.com/en/rest/metrics/statistics?apiVersion=2022-11-28#get-the-weekly-commit-count
      weekly_commit_count_get_request= 'https://api.github.com/repos/ImagingDataCommons/'+repo+'/stats/participation'
      #converting api response to json using authenticated session
      try:
        weekly_commit_count_json = gh_session.get(weekly_commit_count_get_request).json()
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting the total commit counts for the owner and total commit counts in all ')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe
      weekly_commit_count_df=pd.DataFrame()  
      weekly_commit_count_df=pd.DataFrame.from_dict(weekly_commit_count_json)
      #adding a column to indicate the repo_name and renaming columns
      weekly_commit_count_df['repo']=repo
      #adding timestamp of when the data was pulled
      weekly_commit_count_df['timestamp_data_pulled'] = pd.to_datetime('today')
      weekly_commit_count_df=weekly_commit_count_df.rename(columns={'all':'all_commits','owner':'owner_commits'})
      weekly_commit_count_df_appended=pd.concat([weekly_commit_count_df_appended,weekly_commit_count_df])
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived weekly commit count into weekly_commit_count')
      break 
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive the weekly commit count was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue
  
  weekly_commit_count_df_appended

  #hourly_commit_count_for_each_day
  #functions to convert day_number and hour_number to days and readable hours
  def day_number_to_day(x):
    if x==0: return 'Sunday'
    elif x==1: return 'Monday'
    elif x==1: return 'Tuesday'
    elif x==1: return 'Wednesday'  
    elif x==1: return 'Thursday'
    elif x==1: return 'Friday'
    else: return 'Saturday'
  def hour_number_to_hour_name(x):
    if x==0: return '12 AM'
    elif x<=12: return str(x)+' AM'
    else: return str(x-12)+ ' PM'

  for i in range(0,50):
    try:
      #repo='IDC-WebApp'
      #get request for get the hourly commit count for each day of the repository
      #documentation:https://docs.github.com/en/rest/metrics/statistics?apiVersion=2022-11-28#get-the-hourly-commit-count-for-each-day
      hourly_commit_count_for_each_day_get_request= 'https://api.github.com/repos/ImagingDataCommons/'+repo+'/stats/punch_card'
      #converting api response to json using authenticated session
      try:
        hourly_commit_count_for_each_day_json = gh_session.get(hourly_commit_count_for_each_day_get_request).json()
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting the hourly commit count for each day of the repository')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe  
      hourly_commit_count_for_each_day_df=pd.DataFrame() 
      hourly_commit_count_for_each_day_df=pd.DataFrame.from_dict(hourly_commit_count_for_each_day_json)
      #adding a column to indicate the repo_name and renaming columns
      hourly_commit_count_for_each_day_df['repo']=repo
      #adding timestamp of when the data was pulled
      hourly_commit_count_for_each_day_df['timestamp_data_pulled'] = pd.to_datetime('today')
      hourly_commit_count_for_each_day_df=hourly_commit_count_for_each_day_df.rename(columns={0:'day_of_the_week',1:'hour_of_the_day', 2: 'commit_count'})
      hourly_commit_count_for_each_day_df["day_of_the_week"]=hourly_commit_count_for_each_day_df["day_of_the_week"].apply(day_number_to_day)
      hourly_commit_count_for_each_day_df["hour_of_the_day"]=hourly_commit_count_for_each_day_df["hour_of_the_day"].apply(hour_number_to_hour_name)
      hourly_commit_count_for_each_day_df_appended=pd.concat([hourly_commit_count_for_each_day_df_appended,hourly_commit_count_for_each_day_df])
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived weekly commit count into weekly_commit_count')
      break 
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive the hourly commit count for each day was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue

  hourly_commit_count_for_each_day_df_appended

  #pull requests
  for i in range(0,50):
    try:
      #repo='IDC-WebApp'
      #get request for all pull requests of the repository
      #documentation: https://docs.github.com/en/rest/pulls/pulls?apiVersion=2022-11-28#list-pull-requests
      pull_requests_all_get_request= 'https://api.github.com/repos/ImagingDataCommons/'+repo+'/pulls?state=all'
      #converting api response to json using authenticated session
      try:
        pull_requests_all_json = gh_session.get(pull_requests_all_get_request).json()
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting all pull requests of the repository')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe
      pull_requests_all_df=pd.DataFrame()   
      pull_requests_all_df=pd.json_normalize(pull_requests_all_json)
      #adding a column to indicate the repo_name 
      pull_requests_all_df['repo']=repo
      #adding timestamp of when the data was pulled
      pull_requests_all_df['timestamp_data_pulled'] = pd.to_datetime('today')
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived all pull requests into pull_requests_all_df')
      pull_requests_all_df_appended=pd.concat([pull_requests_all_df_appended,pull_requests_all_df])
      break 
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive all pull requests was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue
  pull_requests_all_df_appended
  #Check if a pull request has been merged
  #https://api.github.com/repos/OWNER/REPO/pulls/PULL_NUMBER/merge

  #https://api.github.com/repos/OWNER/REPO/releases
  #releases
  for i in range(0,50):
    try:
      #repo='IDC-WebApp'
      #get request a list of releases of the repository
      #documentation: https://docs.github.com/en/rest/releases/releases?apiVersion=2022-11-28#list-releases
      releases_get_request= 'https://api.github.com/repos/ImagingDataCommons/'+repo+'/releases'
      #converting api response to json using authenticated session
      try:
        releases_get_request_json = gh_session.get(releases_get_request).json()
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting list of releases for the specified repository')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe
      releases_get_request_df=pd.DataFrame()   
      releases_get_request_df=pd.json_normalize(releases_get_request_json)
      #adding a column to indicate the repo_name 
      releases_get_request_df['repo']=repo
      #adding timestamp of when the data was pulled
      releases_get_request_df['timestamp_data_pulled'] = pd.to_datetime('today')
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived list of releases into releases_get_request_df')
      releases_get_request_df_appended=pd.concat([releases_get_request_df_appended,releases_get_request_df])
      break 
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive a list of releases was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue
      

  releases_get_request_df_appended

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
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting list of languages for the specified repository')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe
      list_repository_languages_df=pd.DataFrame()  
      list_repository_languages_df=pd.json_normalize(list_repository_languages_json)
      #adding a column to indicate the repo_name 
      list_repository_languages_df['repo']=repo
      #adding timestamp of when the data was pulled
      list_repository_languages_df['timestamp_data_pulled'] = pd.to_datetime('today')
      list_repository_languages_df_appended=pd.concat([list_repository_languages_df_appended, list_repository_languages_df])
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived list of repository languages into list_repository_languages_df')
      break 
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive a list of repository languages was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue 
  list_repository_languages_df_appended

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
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting list of languages for the specified repository')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ') 
      #converting json to pandas dataframe
      forks_df=pd.DataFrame() 
      forks_df=pd.json_normalize(forks_json)
      #adding a column to indicate the repo_name 
      forks_df['repo']=repo
      #adding timestamp of when the data was pulled
      forks_df['timestamp_data_pulled'] = pd.to_datetime('today')
      forks_df_appended=pd.concat([forks_df_appended,forks_df])
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived list of forks into forks_df')
      break 
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive a list of forks was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue
  forks_df_appended

  #repo_events
  for i in range(0,50):
    try:
      #repo='IDC-WebApp'
      #get request to list repository events
      #documentation:https://docs.github.com/en/rest/activity/events?apiVersion=2022-11-28#list-repository-events
      events= 'https://api.github.com/repos/ImagingDataCommons/'+repo+'/events'
      #converting api response to json using authenticated session
      try:
        events_json = gh_session.get(events).json()
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting list of languages for the specified repository')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe
      events_df=pd.DataFrame()   
      events_df=pd.json_normalize(events_json)
      #adding a column to indicate the repo_name 
      events_df['repo']=repo
      #adding timestamp of when the data was pulled
      events_df['timestamp_data_pulled'] = pd.to_datetime('today')
      events_df_appended=pd.concat([events_df_appended,events_df])
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived list of repository events into events_df')
      break 
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive list of repository events was unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue

  events_df_appended

  for i in range(0,50):
    #branches
    try:
      #repo='IDC-WebApp'
      #get request to list branches of the repository
      #documentation:https://docs.github.com/en/rest/branches/branches?apiVersion=2022-11-28#list-branches
      branches= 'https://api.github.com/repos/ImagingDataCommons/'+repo+'/branches'
      #converting api response to json using authenticated session
      try:
        branches_json = gh_session.get(branches).json()
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting list of branches for the specified repository')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ')
      #converting json to pandas dataframe.
      branches_df=pd.DataFrame()  
      branches_df=pd.json_normalize(branches_json)
      #adding a column to indicate the repo_name 
      branches_df['repo']=repo
      #adding timestamp of when the data was pulled
      branches_df['timestamp_data_pulled'] = pd.to_datetime('today')
      branches_df_appended=pd.concat([branches_df_appended,branches_df])
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived list of branches into branches_df')
      break 
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive list of branches unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue
  branches_df_appended

  #issues
  for i in range(0,50):
    try:
      #repo='IDC-WebApp'
      #get request for a list issues in a repository. Only open issues will be listed by default (used 'state=all' filter to get all issues)
      #Note: GitHub's REST API considers every pull request an issue, but not every issue is a pull request. 
      #For this reason, "Issues" endpoints may return both issues and pull requests in the response. You can identify pull requests by the pull_request key. 
      #Be aware that the id of a pull request returned from "Issues" endpoints will be an issue id. To find out the pull request id, use the "List pull requests" endpoint.
      #documentation:https://docs.github.com/en/rest/issues/issues?apiVersion=2022-11-28#list-repository-issues
      issues= 'https://api.github.com/repos/ImagingDataCommons/'+repo+'/issues?state=all'
      #converting api response to json using authenticated session
      try:
        #issues_json = requests.get(issues, auth=(user_name, github_token)).json()
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication successful while requesting list of issues for the specified repository')
      except:
        print(str(datetime.datetime.now())+' '+repo+' ' +'authentication unsuccessful,perhaps a time out error ') 
      issues_json = gh_session.get(issues).json()
      #converting json to pandas dataframe
      issues_df=pd.DataFrame() 
      issues_df=pd.json_normalize(issues_json)
      #adding a column to indicate the repo_name 
      issues_df['repo']=repo
      #adding timestamp of when the data was pulled
      issues_df['timestamp_data_pulled'] = pd.to_datetime('today')
      issues_df_appended=pd.concat([issues_df_appended,issues_df])
      print(str(datetime.datetime.now())+' '+repo+' ' +'successfully retreived list of issues into issues_df')
      break 
    except:
      print(str(datetime.datetime.now())+' '+repo+' ' +'attempt to retreive list of issues unsuccessful, check for errors while converting json response to dataframe/n')
      print('retrying')
      continue
