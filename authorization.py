#!/usr/bin/env python
# coding: utf-8

# In[5]:


#!pip install --upgrade google-cloud-bigquery
from google.cloud import bigquery
from google.oauth2 import service_account
import os
# TODO(developer): Set key_path to the path to the service account key
#                  file.
# key_path = "path/to/service_account.json"


# In[14]:



client = bigquery.Client()

