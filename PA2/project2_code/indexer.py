'''
@author: Sougata Saha
Institute: University at Buffalo
'''

from linkedlist import LinkedList
from collections import OrderedDict
import csv

class Indexer:
    def __init__(self):
        """ Add more attributes if needed"""
        self.inverted_index = OrderedDict({})
        self.num_docs = 0

    def get_index(self):
        """ Function to get the index.
            Already implemented."""
        return self.inverted_index

    def generate_inverted_index(self, doc_id, tokenized_document):
        """ This function adds each tokenized document to the index. This in turn uses the function add_to_index
            Already implemented."""
        token_count = len(tokenized_document)
        for t in tokenized_document:
            self.add_to_index(t, doc_id, token_count)

    def add_to_index(self, term_, doc_id_, token_count):
        """ This function adds each term & document id to the index.
            If a term is not present in the index, then add the term to the index & initialize a new postings list (linked list).
            If a term is present, then add the document to the appropriate position in the posstings list of the term.
            To be implemented."""
        if term_ in self.inverted_index:
            self.inverted_index[term_].insert_at_end(doc_id_, token_count)
        else:
            self.inverted_index[term_] = LinkedList()
            self.inverted_index[term_].insert_at_end(doc_id_, token_count)

    def sort_terms(self):
        """ Sorting the index by terms.
            Already implemented."""
        sorted_index = OrderedDict({})
        for k in sorted(self.inverted_index.keys()):
            sorted_index[k] = self.inverted_index[k]
        self.inverted_index = sorted_index

    def add_skip_connections(self):
        """ For each postings list in the index, add skip pointers.
            To be implemented."""
        for term in self.inverted_index:
            self.inverted_index[term].add_skip_connections()

    def calculate_tf_idf(self):
        """ Calculate tf-idf score for each document in the postings lists of the index.
            To be implemented."""
        for term in self.inverted_index:
            self.inverted_index[term].add_tf_idf(self.num_docs)

if __name__ == "__main__":
    # doc_terms = {
    #     144285 : ['epidemiolog', 'clinic', 'characterist', '136', 'case', 'covid', '19', 'main', 'district', 'chongq']
    # }

    # doc_terms = {
    #     144285 : ['epidemiolog', 'clinic', 'characterist', '136', 'case', 'covid', '19', 'main', 'district', 'chongq'],
    #     74698 : ['late', 'onset', 'pneumocysti', 'jirovecii', 'pneumonia', 'post', 'fludarabin', 'cyclophosphamid', 'rituximab', 'implic', 'prophylaxi'],
    #     113257 : ['covid', '19', 'diabet', 'mellitu', 'implic', 'prognosi', 'clinic', 'manag'],
    #     156757 : ['seropreval', 'rodent', 'pathogen', 'wild', 'rat', 'island', 'st', 'kitt', 'west', 'indi'],
    #     50439 : ['preval', 'genet', 'divers', 'analysi', 'human', 'coronavirus', 'among', 'cross', 'border', 'children'],
    #     28245 : ['technic', 'consider', 'develop', 'enzym', 'immunohistochem', 'stain', 'procedur', 'formalin', 'fix', 'paraffin', 'embed', 'tissu', 'diagnost', 'patholog'],
    #     10643 : ['persist', 'coronaviru', 'infect', 'progenitor', 'oligodendrocyt'],
    #     102494 : ['divers', 'oppos', 'effect', 'nutrit', 'pathogen', 'virul'],
    #     2598 : ['pharmaceut', 'biotechnolog'],
    #     108895 : ['procalcitonin', 'children', 'suspect', 'novel', 'influenza', 'h1n1', 'infect']
    # }

    # indexer = Indexer()
    # for doc_id in doc_terms:
    #     indexer.generate_inverted_index(doc_id, doc_terms[doc_id])
    # index_ = indexer.get_index()
    # for key_ in index_.keys():
    #     print(key_, end = " - ")
    #     print(index_[key_].traverse_list())

    with open('preprocessor_output.csv','r') as data:
        indexer = Indexer()
        for line in csv.reader(data):
            # print(line)
            indexer.generate_inverted_index(int(line[:1][0]), line[1:])
        index_ = indexer.get_index()
        for key_ in index_.keys():
            print(key_, end = " - ")
            print(index_[key_].traverse_list())
