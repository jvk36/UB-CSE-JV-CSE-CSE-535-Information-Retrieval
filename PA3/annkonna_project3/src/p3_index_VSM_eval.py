import os
import pysolr
import requests
import json

CORE_NAME = "IRF22P3_VSM_eval"
GCP_IP = "localhost"


def delete_core(core=CORE_NAME):
    print(os.system('sudo su - solr -c "/opt/solr/bin/solr delete -c {core}"'.format(core=core)))


def create_core(core=CORE_NAME):
    print(os.system(
        'sudo su - solr -c "/opt/solr/bin/solr create -c {core} -n data_driven_schema_configs"'.format(
            core=core)))


with open('train.json', 'r') as f:
    data = json.load(f)

# ****** DO THE FOLLOWING AFTER RUNNING THIS PROGRAM TO MAKE THE GLOBAL SIMILARITY TO BE SweetSpotSimilarity *******
#
# /var/solr/data/<CORE_NAME>/conf is the location of the core-specific managed-schema.xml & solrconfig.xml files.
#
# 1. Rename the file managed-schema.xml as schema.xml
# 2. Edit the file to add the following lines at the top-level - 
#    This tells solr to use the similarity of text_en field type explicitly 
#    declared as the similarity for all fields not declared with a similarity:
#      <similarity class="solr.SchemaSimilarityFactory">
#         <str name="defaultSimFromFieldType">text_en</str>
#      </similarity>
# 3. Verify that there is NO line like the following in solrconfig.xml file: 
#           <schemaFactory class="ClassicIndexSchemaFactory"/>
#      That line is used to manually manage the schema. The default for this is ManagedIndexSchemaFactory and 
#      so if you have not specified schemaFactory, then it uses ManagedSchema mode. 
# 4.  RESTART solr. At this point, solr console would show managed-schema.xml instead of schema.xml and the 
#      similarity class would be set to the VSM similarity class solr.SweetSpotSimilarityFactory
#
# Note 1: You could modify the managed-schema.xml file directly but is not recommmended.
# Note 2: the default schema file used to create new cores is: /opt/solr/server/solr/configsets/_default/conf/managed-schema.xml
#         Editing the file to add <similarity class="solr.SweetSpotSimilarityFactory"/> BEFORE running this program 
#         is an alternative. But it will result in new cores created using these edited settings.
# Note 3: Switching from Managed Schema (default) to schema.xml and vice-versa is a method mentioned in doc at 
#           https://solr.apache.org/guide/7_5/schema-factory-definition-in-solrconfig.html#classic-schema-xml
#
# *************************************************************
#
# Note: solr.ClassicSimilarityFactory was Lucene's orignal scoring algorithm based on VSM while 
#       solr.SweetSpotSimilarityFactory is an extension that provides additional tuninig options
#
# ******* TO START/STOP solr, DO THE FOLLOWING *********
#
# sudo service solr stop
# sudo service solr start
# sudo service solr status
#
# *******************************************************


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
                {
                    "name": "lang",
                    "type": "text_general",
                    "multiValued": False
                },
                {
                    "name": "text_en",
                    "type": "text_en",
                    "multiValued": False
                }, {
                    "name": "text_de",
                    "type": "text_de",
                    "multiValued": False
                },
                {
                    "name": "text_ru",
                    "type": "text_ru",
                    "multiValued": False
                },
                {
                    "name": "tweet_hashtags",
                    "type": "text_general",
                    "multiValued": True
                },
                {
                    "name": "tweet_urls",
                    "type": "text_general",
                    "multiValued": True
                }
            ]
        }

        print(requests.post(self.solr_url + CORE_NAME + "/schema", json=data).json())


    def replace_VSM(self, baselineTfMin=None, baselineTfBase=None, lengthNormMin=None, lengthNormMax=None, lengthNormSteepness=None):
        data = {
            "replace-field-type": [
                {
                    'name': 'text_en',
                    'class': 'solr.TextField',
                    'positionIncrementGap': '100',
                    'indexAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [
                        {
                            'class': 'solr.StopFilterFactory',
                            'words': 'lang/stopwords_en.txt',
                            'ignoreCase': 'true'
                        }, 
                        {
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.EnglishPossessiveFilterFactory'
                        }, {
                            'class': 'solr.KeywordMarkerFilterFactory',
                            'protected': 'protwords.txt'
                        }
                        , {
                            'class': 'solr.PorterStemFilterFactory'
                        }
                        ]
                    },
                    'similarity': {
                        'class': 'solr.SweetSpotSimilarityFactory'
                    },
                    'queryAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [{
                            'class': 'solr.SynonymGraphFilterFactory',
                            'expand': 'true',
                            'ignoreCase': 'true',
                            'synonyms': 'synonyms.txt'
                        }, 
                        {
                            'class': 'solr.StopFilterFactory',
                            'words': 'lang/stopwords_en.txt',
                            'ignoreCase': 'true'
                        }, 
                        {
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.EnglishPossessiveFilterFactory'
                        }, {
                            'class': 'solr.KeywordMarkerFilterFactory',
                            'protected': 'protwords.txt'
                        }
                        , {
                            'class': 'solr.PorterStemFilterFactory'
                        }
                        ]
                    }
                }, {
                    'name': 'text_ru',
                    'class': 'solr.TextField',
                    'positionIncrementGap': '100',
                    'analyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [{
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.StopFilterFactory',
                            'format': 'snowball',
                            'words': 'lang/stopwords_ru.txt',
                            'ignoreCase': 'true'
                        }, {
                            'class': 'solr.SnowballPorterFilterFactory',
                            'language': 'Russian'
                        }]
                    },
                    'similarity': {
                        'class': 'solr.SweetSpotSimilarityFactory'
                    },
                    'queryAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [{
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.StopFilterFactory',
                            'format': 'snowball',
                            'words': 'lang/stopwords_ru.txt',
                            'ignoreCase': 'true'
                        }, {
                            'class': 'solr.SnowballPorterFilterFactory',
                            'language': 'Russian'
                        }]
                    },
                }, {
                    'name': 'text_de',
                    'class': 'solr.TextField',
                    'positionIncrementGap': '100',
                    'analyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [{
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.StopFilterFactory',
                            'format': 'snowball',
                            'words': 'lang/stopwords_de.txt',
                            'ignoreCase': 'true'
                        }, {
                            'class': 'solr.GermanNormalizationFilterFactory'
                        }, {
                            'class': 'solr.GermanLightStemFilterFactory'
                        }]
                    },
                    'similarity': {
                        'class': 'solr.SweetSpotSimilarityFactory'
                    },
                    'queryAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [{
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.StopFilterFactory',
                            'format': 'snowball',
                            'words': 'lang/stopwords_de.txt',
                            'ignoreCase': 'true'
                        }, {
                            'class': 'solr.GermanNormalizationFilterFactory'
                        }, {
                            'class': 'solr.GermanLightStemFilterFactory'
                        }]
                    }
                }
            ]
        }

        print(requests.post(self.solr_url + CORE_NAME + "/schema", json=data).json())


    def replace_VSM_baseline(self, baselineTfMin=None, baselineTfBase=None, lengthNormMin=None, lengthNormMax=None, lengthNormSteepness=None):
        data = {
            "replace-field-type": [
                {
                    'name': 'text_en',
                    'class': 'solr.TextField',
                    'positionIncrementGap': '100',
                    'indexAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [
                        {
                            'class': 'solr.StopFilterFactory',
                            'words': 'lang/stopwords_en.txt',
                            'ignoreCase': 'true'
                        }, 
                        {
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.EnglishPossessiveFilterFactory'
                        }, {
                            'class': 'solr.KeywordMarkerFilterFactory',
                            'protected': 'protwords.txt'
                        }
                        , {
                            'class': 'solr.PorterStemFilterFactory'
                        }
                        ]
                    },
                    'similarity': {
                        'class': 'solr.SweetSpotSimilarityFactory',
                        'baselineTfMin': str(baselineTfMin),
                        'baselineTfBase': str(baselineTfBase),
                        'lengthNormMin': str(lengthNormMin),
                        'lengthNormMax': str(lengthNormMax),
                        'lengthNormSteepness': str(lengthNormSteepness)
                    },
                    'queryAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [{
                            'class': 'solr.SynonymGraphFilterFactory',
                            'expand': 'true',
                            'ignoreCase': 'true',
                            'synonyms': 'synonyms.txt'
                        }, 
                        {
                            'class': 'solr.StopFilterFactory',
                            'words': 'lang/stopwords_en.txt',
                            'ignoreCase': 'true'
                        }, 
                        {
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.EnglishPossessiveFilterFactory'
                        }, {
                            'class': 'solr.KeywordMarkerFilterFactory',
                            'protected': 'protwords.txt'
                        }
                        , {
                            'class': 'solr.PorterStemFilterFactory'
                        }
                        ]
                    }
                }, {
                    'name': 'text_ru',
                    'class': 'solr.TextField',
                    'positionIncrementGap': '100',
                    'analyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [{
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.StopFilterFactory',
                            'format': 'snowball',
                            'words': 'lang/stopwords_ru.txt',
                            'ignoreCase': 'true'
                        }, {
                            'class': 'solr.SnowballPorterFilterFactory',
                            'language': 'Russian'
                        }]
                    },
                    'similarity': {
                        'class': 'solr.SweetSpotSimilarityFactory',
                        'baselineTfMin': str(baselineTfMin),
                        'baselineTfBase': str(baselineTfBase),
                        'lengthNormMin': str(lengthNormMin),
                        'lengthNormMax': str(lengthNormMax),
                        'lengthNormSteepness': str(lengthNormSteepness)
                    },
                    'queryAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [{
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.StopFilterFactory',
                            'format': 'snowball',
                            'words': 'lang/stopwords_ru.txt',
                            'ignoreCase': 'true'
                        }, {
                            'class': 'solr.SnowballPorterFilterFactory',
                            'language': 'Russian'
                        }]
                    },
                }, {
                    'name': 'text_de',
                    'class': 'solr.TextField',
                    'positionIncrementGap': '100',
                    'analyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [{
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.StopFilterFactory',
                            'format': 'snowball',
                            'words': 'lang/stopwords_de.txt',
                            'ignoreCase': 'true'
                        }, {
                            'class': 'solr.GermanNormalizationFilterFactory'
                        }, {
                            'class': 'solr.GermanLightStemFilterFactory'
                        }]
                    },
                    'similarity': {
                        'class': 'solr.SweetSpotSimilarityFactory',
                        'baselineTfMin': str(baselineTfMin),
                        'baselineTfBase': str(baselineTfBase),
                        'lengthNormMin': str(lengthNormMin),
                        'lengthNormMax': str(lengthNormMax),
                        'lengthNormSteepness': str(lengthNormSteepness)
                    },
                    'queryAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [{
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.StopFilterFactory',
                            'format': 'snowball',
                            'words': 'lang/stopwords_de.txt',
                            'ignoreCase': 'true'
                        }, {
                            'class': 'solr.GermanNormalizationFilterFactory'
                        }, {
                            'class': 'solr.GermanLightStemFilterFactory'
                        }]
                    }
                }
            ]
        }

        print(requests.post(self.solr_url + CORE_NAME + "/schema", json=data).json())


    def replace_VSM_hyperbolic(self, hyperbolicTfMin=None, hyperbolicTfMax=None, hyperbolicTfBase=None, hyperbolicTfOffset=None, lengthNormMin=None, lengthNormMax=None, lengthNormSteepness=None):
        data = {
            "replace-field-type": [
                {
                    'name': 'text_en',
                    'class': 'solr.TextField',
                    'positionIncrementGap': '100',
                    'indexAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [
                        {
                            'class': 'solr.StopFilterFactory',
                            'words': 'lang/stopwords_en.txt',
                            'ignoreCase': 'true'
                        }, 
                        {
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.EnglishPossessiveFilterFactory'
                        }, {
                            'class': 'solr.KeywordMarkerFilterFactory',
                            'protected': 'protwords.txt'
                        }
                        , {
                            'class': 'solr.PorterStemFilterFactory'
                        }
                        ]
                    },
                    'similarity': {
                        'class': 'solr.SweetSpotSimilarityFactory',
                        'hyperbolicTfMin': str(hyperbolicTfMin),
                        'hyperbolicTfMax': str(hyperbolicTfMax),
                        'hyperbolicTfBase': str(hyperbolicTfBase),
                        'hyperbolicTfOffset': str(hyperbolicTfOffset),
                        'lengthNormMin': str(lengthNormMin),
                        'lengthNormMax': str(lengthNormMax),
                        'lengthNormSteepness': str(lengthNormSteepness)
                    },
                    'queryAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [{
                            'class': 'solr.SynonymGraphFilterFactory',
                            'expand': 'true',
                            'ignoreCase': 'true',
                            'synonyms': 'synonyms.txt'
                        }, 
                        {
                            'class': 'solr.StopFilterFactory',
                            'words': 'lang/stopwords_en.txt',
                            'ignoreCase': 'true'
                        }, 
                        {
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.EnglishPossessiveFilterFactory'
                        }, {
                            'class': 'solr.KeywordMarkerFilterFactory',
                            'protected': 'protwords.txt'
                        }
                        , {
                            'class': 'solr.PorterStemFilterFactory'
                        }
                        ]
                    }
                }, {
                    'name': 'text_ru',
                    'class': 'solr.TextField',
                    'positionIncrementGap': '100',
                    'analyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [{
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.StopFilterFactory',
                            'format': 'snowball',
                            'words': 'lang/stopwords_ru.txt',
                            'ignoreCase': 'true'
                        }, {
                            'class': 'solr.SnowballPorterFilterFactory',
                            'language': 'Russian'
                        }]
                    },
                    'similarity': {
                        'class': 'solr.SweetSpotSimilarityFactory',
                        'hyperbolicTfMin': str(hyperbolicTfMin),
                        'hyperbolicTfMax': str(hyperbolicTfMax),
                        'hyperbolicTfBase': str(hyperbolicTfBase),
                        'hyperbolicTfOffset': str(hyperbolicTfOffset),
                        'lengthNormMin': str(lengthNormMin),
                        'lengthNormMax': str(lengthNormMax),
                        'lengthNormSteepness': str(lengthNormSteepness)
                    },
                    'queryAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [{
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.StopFilterFactory',
                            'format': 'snowball',
                            'words': 'lang/stopwords_ru.txt',
                            'ignoreCase': 'true'
                        }, {
                            'class': 'solr.SnowballPorterFilterFactory',
                            'language': 'Russian'
                        }]
                    },
                }, {
                    'name': 'text_de',
                    'class': 'solr.TextField',
                    'positionIncrementGap': '100',
                    'analyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [{
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.StopFilterFactory',
                            'format': 'snowball',
                            'words': 'lang/stopwords_de.txt',
                            'ignoreCase': 'true'
                        }, {
                            'class': 'solr.GermanNormalizationFilterFactory'
                        }, {
                            'class': 'solr.GermanLightStemFilterFactory'
                        }]
                    },
                    'similarity': {
                        'class': 'solr.SweetSpotSimilarityFactory',
                        'hyperbolicTfMin': str(hyperbolicTfMin),
                        'hyperbolicTfMax': str(hyperbolicTfMax),
                        'hyperbolicTfBase': str(hyperbolicTfBase),
                        'hyperbolicTfOffset': str(hyperbolicTfOffset),
                        'lengthNormMin': str(lengthNormMin),
                        'lengthNormMax': str(lengthNormMax),
                        'lengthNormSteepness': str(lengthNormSteepness)
                    },
                    'queryAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [{
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.StopFilterFactory',
                            'format': 'snowball',
                            'words': 'lang/stopwords_de.txt',
                            'ignoreCase': 'true'
                        }, {
                            'class': 'solr.GermanNormalizationFilterFactory'
                        }, {
                            'class': 'solr.GermanLightStemFilterFactory'
                        }]
                    }
                }
            ]
        }

        print(requests.post(self.solr_url + CORE_NAME + "/schema", json=data).json())


if __name__ == "__main__":
    # delete_core()
    i = Indexer()
    i.do_initial_setup()
    i.add_fields()
    # i.replace_VSM() # all 0.7117, same as with ClassicSimilarityFactory
    # all 0.7000
    # i.replace_VSM_baseline(baselineTfMin=6.0, baselineTfBase=1.5, lengthNormMin=3, lengthNormMax=5, lengthNormSteepness=0.5) 
    # all 0.7133
    i.replace_VSM_hyperbolic(hyperbolicTfMin=3.3, hyperbolicTfMax=7.7, hyperbolicTfBase=2.72, hyperbolicTfOffset=5.0, lengthNormMin=1, lengthNormMax=5, lengthNormSteepness=0.2) 
    i.create_documents(data)
