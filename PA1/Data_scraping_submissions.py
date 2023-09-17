#!/usr/bin/env python
# coding: utf-8

# In[50]:


import requests
import pandas as pd
import re
from datetime import datetime


# In[51]:


def get_data(url):
    a = requests.get(url)
    return a.json()
def convert_date(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ')


# In[52]:


get_ipython().system('pip install psaw')


# In[53]:


from psaw import PushshiftAPI
api = PushshiftAPI()


# In[54]:


gen_ExplainLikeImFive = list(api.search_submissions(subreddit=['ExplainLikeImFive'], 
                                                filter=['id', 'subreddit', 'full_link', 'title', 'selftext',
                                                       'author', 'created_utc'],
                                                limit=100, selftext="not [removed]" and 
                                                   "not [deleted]" and 'not "" '))
gen_FoodForThought = list(api.search_submissions(subreddit=['FoodForThought'], 
                                                filter=['id', 'subreddit', 'full_link', 'title', 'selftext',
                                                       'author', 'created_utc'],
                                                limit=100, selftext="not [removed]" and 
                                                   "not [deleted]" and 'not "" '))
gen_ChangeMyView = list(api.search_submissions(subreddit=['ChangeMyView'], 
                                                filter=['id', 'subreddit', 'full_link', 'title', 'selftext',
                                                       'author', 'created_utc'],
                                                limit=100, selftext="not [removed]" and 
                                                   "not [deleted]" and 'not "" '))
gen_TodayILearned = list(api.search_submissions(subreddit=['TodayILearned'], 
                                                filter=['id', 'subreddit', 'full_link', 'title', 'selftext',
                                                       'author', 'created_utc'],
                                                limit=100, selftext="not [removed]" and 
                                                   "not [deleted]" and 'not "" '))
gen_politics = list(api.search_submissions(q='democracy', subreddit=['politics'], 
                                                filter=['id', 'subreddit', 'full_link', 'title', 'selftext',
                                                       'author', 'created_utc'],
                                                limit=200, selftext="not [removed]" and 
                                                   "not [deleted]" and 'not "" '))
gen_technology = list(api.search_submissions(q='tech', subreddit=['technology'], 
                                                filter=['id', 'subreddit', 'full_link', 'title', 'selftext',
                                                       'author', 'created_utc'],
                                                limit=200, selftext="not [removed]" and 
                                                   "not [deleted]" and 'not "" '))
gen_environment = list(api.search_submissions(q='climate change', subreddit=['environment'], 
                                                filter=['id', 'subreddit', 'full_link', 'title', 'selftext',
                                                       'author', 'created_utc'],
                                                limit=200, selftext="not [removed]" and 
                                                   "not [deleted]" and 'not "" '))
gen_healthcare_1 = list(api.search_submissions(q='value based care', subreddit=['healthcare'], 
                                                filter=['id', 'subreddit', 'full_link', 'title', 'selftext',
                                                       'author', 'created_utc'],
                                                limit=200, selftext="not [removed]" and 
                                                   "not [deleted]" and 'not "" '))
gen_healthcare_2 = list(api.search_submissions(q='medical insurance', subreddit=['healthcare'], 
                                                filter=['id', 'subreddit', 'full_link', 'title', 'selftext',
                                                       'author', 'created_utc'],
                                                limit=200, selftext="not [removed]" and 
                                                   "not [deleted]" and 'not "" '))
gen_education = list(api.search_submissions(q='university', subreddit=['education'], 
                                                filter=['id', 'subreddit', 'full_link', 'title', 'selftext',
                                                       'author', 'created_utc'],
                                                limit=200, selftext="not [removed]" and 
                                                   "not [deleted]" and 'not "" '))
gen_bangalore_500 = list(api.search_submissions(q='bangalore', subreddit=['india'], 
                                                filter=['id', 'subreddit', 'full_link', 'title', 'selftext',
                                                       'author', 'created_utc'],
                                                limit=500, selftext="not [removed]" and 
                                                   "not [deleted]" and 'not "" '))
gen_kerala_500 = list(api.search_submissions(q='kerala', subreddit=['india'], 
                                                filter=['id', 'subreddit', 'full_link', 'title', 'selftext',
                                                       'author', 'created_utc'],
                                                limit=500, selftext="not [removed]" and 
                                                   "not [deleted]" and 'not "" '))
gen_gujarat_500 = list(api.search_submissions(q='gujarat', subreddit=['india'], 
                                                filter=['id', 'subreddit', 'full_link', 'title', 'selftext',
                                                       'author', 'created_utc'],
                                                limit=500, selftext="not [removed]" and 
                                                   "not [deleted]" and 'not "" '))
gen_chennai_500 = list(api.search_submissions(q='chennai', subreddit=['india'], 
                                                filter=['id', 'subreddit', 'full_link', 'title', 'selftext',
                                                       'author', 'created_utc'],
                                                limit=500, selftext="not [removed]" and 
                                                   "not [deleted]" and 'not "" '))
gen_goa_500 = list(api.search_submissions(q='goa', subreddit=['india'], 
                                                filter=['id', 'subreddit', 'full_link', 'title', 'selftext',
                                                       'author', 'created_utc'],
                                                limit=500, selftext="not [removed]" and 
                                                   "not [deleted]" and 'not "" '))


# In[55]:


submission_df_ExplainLikeImFive = pd.DataFrame(gen_ExplainLikeImFive)
submission_df_FoodForThought = pd.DataFrame(gen_FoodForThought)
submission_df_ChangeMyView = pd.DataFrame(gen_ChangeMyView)
submission_df_TodayILearned = pd.DataFrame(gen_TodayILearned)
submission_df_politics = pd.DataFrame(gen_politics)
submission_df_technology = pd.DataFrame(gen_technology)
submission_df_environment = pd.DataFrame(gen_environment)
submission_df_healthcare_1 = pd.DataFrame(gen_healthcare_1)
submission_df_healthcare_2 = pd.DataFrame(gen_healthcare_2)
submission_df_education = pd.DataFrame(gen_education)
submission_df_bangalore_500 = pd.DataFrame(gen_bangalore_500)
submission_df_kerala_500 = pd.DataFrame(gen_kerala_500)
submission_df_gujarat_500 = pd.DataFrame(gen_gujarat_500)
submission_df_chennai_500 = pd.DataFrame(gen_chennai_500)
submission_df_goa_500 = pd.DataFrame(gen_goa_500)


# In[56]:


submission_df_bangalore_500


# In[57]:


submission_df_kerala_500


# In[58]:


submission_df_gujarat_500


# In[59]:


submission_df_chennai_500


# In[60]:


submission_df_goa_500


# In[61]:


submission_df_ExplainLikeImFive


# In[62]:


submission_df_FoodForThought


# In[63]:


submission_df_ChangeMyView


# In[64]:


submission_df_TodayILearned


# In[65]:


submission_df_politics


# In[66]:


submission_df_technology


# In[67]:


submission_df_environment


# In[68]:


submission_df_education


# In[69]:


submission_df_healthcare_1


# In[70]:


submission_df_healthcare_2


# In[71]:


submission_df_total = pd.concat([submission_df_ExplainLikeImFive, submission_df_FoodForThought, submission_df_ChangeMyView, 
                                 submission_df_TodayILearned, submission_df_politics, submission_df_technology, 
                                 submission_df_environment, submission_df_healthcare_1, submission_df_healthcare_2, 
                                 submission_df_education, submission_df_bangalore_500, submission_df_kerala_500, 
                                 submission_df_chennai_500, submission_df_gujarat_500, submission_df_goa_500])


# In[72]:


is_submission_lst = []
topic_lst = []
created_at_lst = []
for s in submission_df_total['created_utc']:
    created_at_lst.append(convert_date(s))
    # topic_lst.append(s['topic'])
    is_submission_lst.append(True)
# submission_df_total['topic']
for t in submission_df_total['subreddit']:
    if t == "politics":
        topic_lst.append("Politics")
    elif t == "environment":
        topic_lst.append("Environment")
    elif t == "technology":
        topic_lst.append("Technology")
    elif t == "healthcare":
        topic_lst.append("Healthcare")
    elif t == "education":
        topic_lst.append("Education")
    else:
        topic_lst.append("")


# In[73]:


submission_df_total.pop("created_utc")
submission_df_total.pop("d_")
submission_df_total.pop("created")
submission_df_total['is_submission'] = is_submission_lst
submission_df_total['topic'] = topic_lst
submission_df_total['created_at'] = created_at_lst


# In[74]:


submission_df_total


# In[75]:


submission_df_total.to_pickle("submissions.pkl")


# In[ ]:




