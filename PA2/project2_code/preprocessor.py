'''
@author: Sougata Saha
Institute: University at Buffalo
'''

import collections
from nltk.stem import PorterStemmer
import re
from nltk.corpus import stopwords
import nltk

from tqdm import tqdm
import re
import csv

nltk.download('stopwords')


class Preprocessor:
    def __init__(self):
        self.stop_words = set(stopwords.words('english'))
        self.ps = PorterStemmer()

    def get_doc_id(self, doc):
        """ Splits each line of the document, into doc_id & text.
            Already implemented"""
        arr = doc.split("\t")
        return int(arr[0]), arr[1]

    
    def _remove_special_chars(self, text):
        match_string = r"[^0-9a-zA-Z\s]+"
        return re.sub(match_string, " ", text)


    def _remove_excess_whitespaces(self, text):
        text = text.strip()
        match_string = r"[\s\s]+"
        return re.sub(match_string, " ", text)


    def tokenizer(self, text):
        """ Implement logic to pre-process & tokenize document text.
            Write the code in such a way that it can be re-used for processing the user's query.
            To be implemented."""
        text = text.lower()
        text = self._remove_special_chars(text)
        text = self._remove_excess_whitespaces(text)
        tokens = re.split(r'\s', text)
        tokens = [word for word in tokens if word not in self.stop_words]
        stemmed_tokens = [self.ps.stem(token) for token in tokens]
        return stemmed_tokens


if __name__ == "__main__":
    preprocessor = Preprocessor()
    with open('data/input_corpus.txt', 'r') as fp:
        lines_to_process=5500
        count=0
        doc_tokens = []
        for line in tqdm(fp.readlines()):
            count = count + 1
            doc_id, document = preprocessor.get_doc_id(line)
            tokenized_document = preprocessor.tokenizer(document)
            tokenized_document.insert(0, doc_id)
            doc_tokens.append(tokenized_document)
            # print(f"{doc_id}, {tokenized_document}")
            if count >= lines_to_process: 
                break
    with open('preprocessor_output.csv', 'w', newline='') as myfile:
        writer = csv.writer(myfile)
        # writer.writerow(myheaders)
        writer.writerows(doc_tokens)
