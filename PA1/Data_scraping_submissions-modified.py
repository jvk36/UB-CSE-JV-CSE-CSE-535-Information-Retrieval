#!/usr/bin/env python
# coding: utf-8

# In[7]:


import requests
import pandas as pd
import re
from datetime import datetime
import itertools


# In[8]:


def convert_date(timestamp):
    # change the created_utc column date format from UTC to YYYY-MM-DDThh:mm:ssZ
    # return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S')
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ')


def display_df(df):
    pd.options.display.width = None
    pd.options.display.max_columns = None
    pd.set_option('display.max_rows', 3000)
    pd.set_option('display.max_columns', 3000)
    print(df)


# In[9]:


get_ipython().system('pip install psaw')


# In[10]:


from psaw import PushshiftAPI
api = PushshiftAPI()


# In[11]:


subreddits = ["ExplainLikeImFive", "FoodForThought", "ChangeMyView", "TodayILearned"]

dframes = []
for item in subreddits:
    topic = []
    gen = api.search_submissions(subreddit=[item],
                             filter=['id', 'subreddit', 'full_link', 'title', 'author', 'created_utc',  'selftext'],
                             limit=110,  # before='400d',
                             selftext="not [removed]" and "not [deleted]" and 'not "" ')
    df = pd.DataFrame(gen)
    topic.extend([""] * len(df.index))
    df['topic'] = topic
    dframes.append(df)

# PART 1: Data Ingestion Requirements
# 2. Get a minimum of 200 submissions for each of these topics:
#   - Politics, Environment, Technology, Healthcare, and Education.
#   - You are required to come up with custom keywords to get submissions related to these
#       topics.
topic_keywords = [
    # politics
    "democracy", "socialism", "dictatorship", "imperialism",
    # environment
    "esg", "epa", "pollution", "climate change",
    # technology
    "tech", "saas", "social media", "blockchain",
    # healthcare
    "medical insurance", "value based care", "affordable care act", "medicare advantage",
    # education
    "university", "homeschooling", "special education", "k12"
]

topics = [*["Politics"] * 4, *["Environment"] * 4, *["Technology"] * 4, *["Healthcare"] * 4, *["Education"] * 4]
for (item, t_keyword) in itertools.zip_longest(topic_keywords, topics):
    topic = []
    gen = api.search_submissions(q=item,
                                 filter=['id', 'subreddit', 'full_link', 'title', 'author', 'created_utc', 'selftext'],
                                 limit=90, # before='400d',
                                 selftext="not [removed]" and "not [deleted]" and 'not "" ')
    df = pd.DataFrame(gen)
    topic.extend([t_keyword] * len(df.index))
    df['topic'] = topic
    dframes.append(df)

# PART 1: Data Ingestion Requirements
# 3. Get a minimum of 2,000 submissions
topics_2k_min = ["metaverse", "crypto", "nft", "tesla", "bitcoin", "google",
          "twitter", "instagram", "tiktok", "facebook", "amazon", "snapchat"]
for item in topics_2k_min:
    topic = []
    gen = api.search_submissions(q=item,
                                 filter=['id', 'subreddit', 'full_link', 'title', 'author', 'created_utc', 'selftext'],
                                 limit=200, # before='400d',
                                 selftext="not [removed]" and "not [deleted]" and 'not "" ')
    df = pd.DataFrame(gen)
    topic.extend([""] * len(df.index))
    df['topic'] = topic
    dframes.append(df)

df = pd.concat(dframes)
created_at = []
submission_col = []
for s in df['created_utc']:
    created_at.append(convert_date(s))
    submission_col.append(True)
df.pop('created_utc')
df.pop('d_')
df.pop('created')
df['created_at'] = created_at
df['is_submission'] = submission_col

print(len(df.index))
display_df(df)

df.to_pickle("psaw-submissions.pkl")


# In[ ]:




