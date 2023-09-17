#!/usr/bin/env python
# coding: utf-8

# In[7]:


get_ipython().system('pip install pysolr')


# In[15]:


import os
import pysolr
import requests
import pandas as pd


# In[9]:


collection = pd.read_pickle("submissions.pkl").to_dict("records")
collection


# In[16]:


CORE_NAME = "IRF22_class_demo_test"
GCP_IP = "localhost"


def delete_core(core=CORE_NAME):
    print(os.system('sudo su - solr -c "/opt/solr/bin/solr delete -c {core}"'.format(core=core)))


def create_core(core=CORE_NAME):
    print(os.system(
        'sudo su - solr -c "/opt/solr/bin/solr create -c {core} -n data_driven_schema_configs"'.format(
            core=core)))


# collection

collection_submissions = pd.read_pickle("psaw-submissions.pkl")[["author", "full_link", "id", "selftext", "subreddit", "title",  
                                                "topic", "created_at", "is_submission"]].to_dict("records")
collection_comments = pd.read_pickle("psaw-submissions.pkl")[["author", "body", "id", "parent_id", "full_link", "subreddit", 
                                                              "parent_body", "created_at", "is_submission"]].to_dict("records")


class Indexer:
    def __init__(self):
        self.solr_url = f'http://{GCP_IP}:8983/solr/'
        self.connection = pysolr.Solr(self.solr_url + CORE_NAME, always_commit=True, timeout=5000000)

    def do_initial_setup(self):
        delete_core()
        create_core()

    def create_documents(self, docs):
        print(self.connection.add(docs))


if __name__ == "__main__":
    i = Indexer()
    i.do_initial_setup()
    i.create_documents(collection_submissions)
    i.create_documents(collection_comments)


# In[ ]:




