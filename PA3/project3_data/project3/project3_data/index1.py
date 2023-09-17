import os
import pysolr
import requests
import pickle
import json

CORE_NAME = "IRF22P3"
GCP_IP = "localhost"


def delete_core(core=CORE_NAME):
    print(os.system('sudo su - solr -c "/opt/solr/bin/solr delete -c {core}"'.format(core=core)))


def create_core(core=CORE_NAME):
    print(os.system(
        'sudo su - solr -c "/opt/solr/bin/solr create -c {core} -n data_driven_schema_configs"'.format(
            core=core)))


# collection1 - submissions

with open('train.json', 'r') as f:
    data = json.load(f)


class Indexer:
    def __init__(self):
        self.solr_url = f'http://{GCP_IP}:8983/solr/'
        self.connection = pysolr.Solr(self.solr_url + CORE_NAME, always_commit=True, timeout=5000000)

    def do_initial_setup(self):
        delete_core()
        create_core()

    def create_documents(self, docs):
        print(self.connection.add(docs))

    def add_fields(self):
        data = {
            "add-field": [
                # fields for submissions doc
                {
                    "name": "subreddit",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "full_link",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "title",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "selftext",
                    "type": "text_en",
                    "multiValued": False
                },
                {
                    "name": "author",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "is_submission",
                    "type": "boolean",
                    "multiValued": False
                },
                {
                    "name": "topic",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "created_at",
                    "type": "pdate",
                    "multiValued": False
                },
                # additional fields for comments doc
                {
                    "name": "body",
                    "type": "text_en",
                    "multiValued": False
                },
                {
                    "name": "parent_id",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "parent_body",
                    "type": "text_en",
                    "multiValued": False
                },
            ]
        }

        print(requests.post(self.solr_url + CORE_NAME + "/schema", json=data).json())


if __name__ == "__main__":
    i = Indexer()
    i.do_initial_setup()
    # i.add_fields()
    i.create_documents(data)
    # i.create_documents(collection2)