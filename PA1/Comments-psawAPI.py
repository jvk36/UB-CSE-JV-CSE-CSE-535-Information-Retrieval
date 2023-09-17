import time
import requests
import json
from datetime import datetime
import pandas as pd
import itertools
from psaw import PushshiftAPI
import pickle


def convert_date(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ')


def display_df(df):
    pd.options.display.width = None
    pd.options.display.max_columns = None
    pd.set_option('display.max_rows', 3000)
    pd.set_option('display.max_columns', 3000)
    print(df)


api = PushshiftAPI()

# fields parameter details for comments -
# *** DIRECT FIELDS: fields - id, subreddit, full_link, body, author, parent_id
# *** CUSTOM FIELDS - parent_body, is_submission, created_at - first two are TO DO
# *** converted created_utc to created_at using the format YYYY-MM-DDThh:mm:ssZ

# comments_lst = list(api.search_comments(link_id=submission_df["id"].iloc[0], subreddit=['ExplainLikeImFive'],
#                                         filter=['id','parent_id','permalink','author', 'title',
#                                                 'subreddit','body','num_comments','score'], limit=20))
with open('psaw-submissions.pkl', 'rb') as f:
    submissions_df = pickle.load(f)
# display_df(submissions_df)
# print(len(submissions_df.index))

dframes = []
count = 0
df = pd.DataFrame()
for (s_id, s_subr, s_selftext) in zip(submissions_df.id, submissions_df.subreddit, submissions_df.selftext):
    gen = api.search_comments(
        link_id=s_id, subreddit=[s_subr],
        filter=['id', 'parent_id', 'permalink', 'author', 'subreddit', 'body', 'created_utc'],
        body="not [removed]" and "not [deleted]" and 'not "" '
    )
    df = pd.DataFrame(gen)
    if len(df.index) == 0:
        continue
    df = df.loc[(df['body'] != '[deleted]') & (df['body'] != '[removed]') & (df['body'] != '')]
    parent_body = []
    for parent_id in df.parent_id:
        if s_id == parent_id[3:]:
            parent_body.append(s_selftext)
        else:
            parent_body.append("")
    count = count + len(df.index)
    df['parent_body'] = parent_body
    dframes.append(df)
    if count > 80000:
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

df = df.loc[(df['parent_body'] != '[deleted]') & (df['parent_body'] != '[removed]') & (df['parent_body'] != '')]

# **** code to strip first 3 characters of parent_id column ****
# print(len(df['body']))
# df['parent_id'] = df['parent_id'].str[3:]

# **** code to replace date with different format
# print(len(df['body']))
# df['created_at'] = df['created_at'] + 'Z'

# *** code to rename the permalink column as full_link
df = df.rename(columns={'permalink': 'full_link'})

print(len(df.parent_body))
# display_df(df)

df.to_pickle("psaw-comments.pkl")
