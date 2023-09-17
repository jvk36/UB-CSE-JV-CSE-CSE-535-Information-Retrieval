# -*- coding: utf-8 -*-


import json
# if you are using python 3, you should 
import urllib.request 
# import urllib2
import urllib.parse

# from translate import Translator
import googletrans
from googletrans import Translator

CORE_NAME = "IRF22P3_VSM_eval" # IRF22P3_VSM_EVAL or IRF22P3_BM25_EVAL
GCP_IP = "localhost"
MAX_ROWS = 20  # &rows=n parameter - use to restrict rows, default is 10
QUERY_PARSER = "" # "defType=edismax&" # 


IRModel='vsm' #either bm25 or vsm
outfn = 'VSM_trec_eval_input.txt' # trec_input_eval.txt or trec_input_vsm_eval.txt


translator = Translator()

qraw = [
    "Anti-Refugee Rally in Dresden",
    "Syrian civil war",
    "Assad und ISIS auf dem Vormarsch",
    "Russische Botschaft in Syrien von Granaten getroffen",
    "Бильд. Внутренний документ говорит, что Германия примет 1,5 млн беженцев в этом году"
]

queries = [
    f"text_en:({qraw[0]}) text_de:({translator.translate(qraw[0],src='en',dest='de').text}) text_ru:({translator.translate(qraw[0],src='en',dest='ru').text})",
    f"text_en:({qraw[1]}) text_de:({translator.translate(qraw[1],src='en',dest='de').text}) text_ru:({translator.translate(qraw[1],src='en',dest='ru').text})",
    f"text_de:({qraw[2]}) text_en:({translator.translate(qraw[2],src='de',dest='en').text}) text_ru:({translator.translate(qraw[2],src='de',dest='ru').text})",
    f"text_de:({qraw[3]}) text_en:({translator.translate(qraw[3],src='de',dest='en').text}) text_ru:({translator.translate(qraw[3],src='de',dest='ru').text})",
    f"text_ru:({qraw[4]}) text_en:({translator.translate(qraw[4],src='ru',dest='en').text}) text_de:({translator.translate(qraw[4],src='ru',dest='de').text})"
]

num_queries = len(queries)

outf = open(outfn, 'w') # 'a+') # 
for l in range(num_queries):
    n=l+1
    # change the url according to your own corename and query
    inurl = f"http://{GCP_IP}:8983/solr/{CORE_NAME}/select?{QUERY_PARSER}fl=id%2Cscore&wt=json&indent=true&rows={MAX_ROWS}&q={urllib.parse.quote(queries[l])}" 
    # qid = str(n).zfill(3)
    qid = f"{n:03}"
    # data = urllib2.urlopen(inurl)
    # if you're using python 3, you should use
    data = urllib.request.urlopen(inurl)

    docs = json.load(data)['response']['docs']
    # the ranking should start from 1 and increase
    rank = 1
    for doc in docs:
        outf.write(qid + ' ' + 'Q0' + ' ' + str(doc['id']) + ' ' + str(rank) + ' ' + str(doc['score']) + ' ' + IRModel + '\n')
        rank += 1
outf.close()