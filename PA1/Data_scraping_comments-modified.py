#!/usr/bin/env python
# coding: utf-8

# In[29]:


import time
import requests
import json
from datetime import datetime
import pandas as pd
import itertools
import pickle


# In[30]:


def convert_date(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S')


def display_df(df):
    pd.options.display.width = None
    pd.options.display.max_columns = None
    pd.set_option('display.max_rows', 3000)
    pd.set_option('display.max_columns', 3000)
    print(df)


# In[31]:


get_ipython().system('pip install psaw')


# In[32]:


from psaw import PushshiftAPI
api = PushshiftAPI()


# In[33]:


with open('psaw-submissions.pkl', 'rb') as f:
    submissions_df = pickle.load(f)
# display_df(submissions_df)
# print(len(submissions_df.index))

dframes = []
count = 0
df = pd.DataFrame()
for (s_id, s_subr, s_body) in zip(submissions_df.id, submissions_df.subreddit, submissions_df.selftext):
    gen = api.search_comments(
        link_id=s_id, subreddit=[s_subr],
        filter=['id', 'parent_id', 'permalink', 'author', 'subreddit', 'body', 'created_utc'],
        body="not [removed]" and "not [deleted]" and 'not "" ')
    df = pd.DataFrame(gen)
    if len(df.index) == 0:
        continue
    parent_body = []
    for parent_id in df.parent_id:
        if s_id == parent_id:
            parent_body.append(s_body)
        else:
            parent_body.append("")
    count = count + len(df.index)
    df['parent_body'] = parent_body
    dframes.append(df)
    if count > 27100:
        break

df = pd.concat(dframes)

for id, parent_id in zip(df.id, df.parent_id):
    parent_body = df.loc[df['id'] == parent_id[3:], 'body']
    if len(parent_body) == 0:
        continue
    df.loc[df['id'] == id, 'parent_body'] = parent_body.to_string(index=False)

created_at = []
is_submission = []
for s in df['created_utc']:
    created_at.append(convert_date(s))
    is_submission.append(False)
df.pop('created_utc')
df.pop('d_')
df.pop('created')
df['created_at'] = created_at
df['is_submission'] = is_submission
df.rename(columns ={"permalink": "full_link"}, inplace = True)

print(len(df.index))
display_df(df)

df.to_pickle("psaw-comments.pkl")


# In[ ]:




