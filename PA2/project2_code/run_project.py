'''
@author: Sougata Saha
Institute: University at Buffalo
'''

from tqdm import tqdm
from preprocessor import Preprocessor
from indexer import Indexer
from collections import OrderedDict
from linkedlist import LinkedList
import inspect as inspector
import sys
import argparse
import json
import time
import random
import flask
from flask import Flask
from flask import request
import hashlib

app = Flask(__name__)


class ProjectRunner:
    def __init__(self):
        self.preprocessor = Preprocessor()
        self.indexer = Indexer()

    def _merge(self, p_list1, p_list2, skip=False):
        """ Implement the merge algorithm to merge 2 postings list at a time.
            Use appropriate parameters & return types.
            While merging 2 postings list, preserve the maximum tf-idf value of a document.
            To be implemented."""
        # raise NotImplementedError
        merged_list = LinkedList()
        cur_node1 = p_list1.start_node
        cur_node2 = p_list2.start_node
        
        comparisons = 0
        while cur_node1 is not None and cur_node2 is not None:
            while cur_node1 is not None and cur_node1.value < cur_node2.value:
                comparisons += 1
                cur_next = cur_node1.next
                cur_skip = cur_node1.skip
                if skip and cur_skip is not None and cur_skip.value < cur_node2.value:
                    cur_node1 = cur_skip
                else:
                    cur_node1 = cur_next
            if cur_node1 is None:
                break
            while cur_node2 is not None and cur_node2.value < cur_node1.value:
                comparisons += 1
                cur_next = cur_node2.next
                cur_skip = cur_node2.skip
                if skip and cur_skip is not None and cur_skip.value < cur_node1.value:
                    cur_node2 = cur_skip
                else:
                    cur_node2 = cur_next
            if cur_node2 is None:
                break
            if cur_node1.value == cur_node2.value:
                comparisons += 1
                node_to_merge = cur_node1 if cur_node1.tf_idf_score > cur_node2.tf_idf_score else cur_node2
                merged_list.append(node_to_merge)
                cur_node1 = cur_node1.next
                cur_node2 = cur_node2.next
        return merged_list, comparisons

    def _daat_and(self, query_terms, skip=False, sorted_=False):
        """ Implement the DAAT AND algorithm, which merges the postings list of N query terms.
            Use appropriate parameters & return types.
            To be implemented."""
        # raise NotImplementedError
        if len(query_terms) < 1:
            return [], 0

        list_to_merge = []
        for query in query_terms:
            list_to_merge.append(self.indexer.inverted_index[query])
        list_to_merge.sort(key=lambda x: x.length)

        p_list1 = list_to_merge[0]
        list_to_merge = list_to_merge[1:]
        total_comparisons = 0
        while len(list_to_merge) >= 1:
            p_list2 = list_to_merge[0]
            p_list1, comparisons = self._merge(p_list1, p_list2, skip)
            list_to_merge = list_to_merge[1:]
            total_comparisons += comparisons

        if sorted_:
            p_list1.sort_by_tfidf()

        p_ret_list = []
        c_node = p_list1.start_node
        while c_node is not None:
            p_ret_list.append(c_node.value)
            c_node = c_node.next

        # if sorted_:
        #     p_ret_list.sort(key=lambda x: x.tf_idf, reverse=True)
        
        # p_ret_list = [item.value for item in p_ret_list]
        return p_ret_list, total_comparisons

    def _get_postings(self, term, skip=False):
        """ Function to get the postings list of a term from the index.
            Use appropriate parameters & return types.
            To be implemented."""
        # raise NotImplementedError
        if skip:
            return self.indexer.inverted_index[term].traverse_skips()
        else:
            return self.indexer.inverted_index[term].traverse_list()

    def _output_formatter(self, op):
        """ This formats the result in the required format.
            Do NOT change."""
        if op is None or len(op) == 0:
            return [], 0
        op_no_score = [int(i) for i in op]
        results_cnt = len(op_no_score)
        return op_no_score, results_cnt

    def run_indexer(self, corpus):
        """ This function reads & indexes the corpus. After creating the inverted index,
            it sorts the index by the terms, add skip pointers, and calculates the tf-idf scores.
            Already implemented, but you can modify the orchestration, as you seem fit."""
        with open(corpus, 'r') as fp:
            count = 0
            for line in tqdm(fp.readlines()):
                count = count + 1
                doc_id, document = self.preprocessor.get_doc_id(line)
                tokenized_document = self.preprocessor.tokenizer(document)
                self.indexer.generate_inverted_index(doc_id, tokenized_document)
        self.indexer.num_docs = count
        self.indexer.sort_terms()
        self.indexer.add_skip_connections()
        self.indexer.calculate_tf_idf()

    def sanity_checker(self, command):
        """ DO NOT MODIFY THIS. THIS IS USED BY THE GRADER. """

        index = self.indexer.get_index()
        kw = random.choice(list(index.keys()))
        return {"index_type": str(type(index)),
                "indexer_type": str(type(self.indexer)),
                "post_mem": str(index[kw]),
                "post_type": str(type(index[kw])),
                "node_mem": str(index[kw].start_node),
                "node_type": str(type(index[kw].start_node)),
                "node_value": str(index[kw].start_node.value),
                "command_result": eval(command) if "." in command else ""}

    def run_queries(self, query_list, random_command):
        """ DO NOT CHANGE THE output_dict definition"""
        output_dict = {'postingsList': {},
                       'postingsListSkip': {},
                       'daatAnd': {},
                       'daatAndSkip': {},
                       'daatAndTfIdf': {},
                       'daatAndSkipTfIdf': {},
                       'sanity': self.sanity_checker(random_command)}

        for query in tqdm(query_list):
            """ Run each query against the index. You should do the following for each query:
                1. Pre-process & tokenize the query.
                2. For each query token, get the postings list & postings list with skip pointers.
                3. Get the DAAT AND query results & number of comparisons with & without skip pointers.
                4. Get the DAAT AND query results & number of comparisons with & without skip pointers, 
                    along with sorting by tf-idf scores."""
            # raise NotImplementedError

            input_term_arr = self.preprocessor.tokenizer(query)  # Tokenized query. To be implemented.

            for term in input_term_arr:
                postings, skip_postings = self._get_postings(term), self._get_postings(term, skip=True)

                """ Implement logic to populate initialize the above variables.
                    The below code formats your result to the required format.
                    To be implemented."""

                output_dict['postingsList'][term] = postings
                output_dict['postingsListSkip'][term] = skip_postings

            and_op_no_skip, and_comparisons_no_skip = self._daat_and(input_term_arr)
            and_op_skip, and_comparisons_skip = self._daat_and(input_term_arr, skip=True)
            and_op_no_skip_sorted, and_comparisons_no_skip_sorted = self._daat_and(input_term_arr, sorted_=True)
            and_op_skip_sorted, and_comparisons_skip_sorted = self._daat_and(input_term_arr, skip=True, sorted_=True)
            # and_op_skip, and_comparisons_skip = self._daat_and(input_term_arr, skip=True)
            # and_op_no_skip_sorted, and_comparisons_no_skip_sorted = self._daat_and(input_term_arr, skip=False, sorted_=True)
            # and_op_skip_sorted, and_comparisons_skip_sorted = self._daat_and(input_term_arr, skip=True, sorted_=True)
            """ Implement logic to populate initialize the above variables.
                The below code formats your result to the required format.
                To be implemented."""
            and_op_no_score_no_skip, and_results_cnt_no_skip = self._output_formatter(and_op_no_skip)
            and_op_no_score_skip, and_results_cnt_skip = self._output_formatter(and_op_skip)
            and_op_no_score_no_skip_sorted, and_results_cnt_no_skip_sorted = self._output_formatter(and_op_no_skip_sorted)
            and_op_no_score_skip_sorted, and_results_cnt_skip_sorted = self._output_formatter(and_op_skip_sorted)

            output_dict['daatAnd'][query.strip()] = {}
            output_dict['daatAnd'][query.strip()]['results'] = and_op_no_score_no_skip
            output_dict['daatAnd'][query.strip()]['num_docs'] = and_results_cnt_no_skip
            output_dict['daatAnd'][query.strip()]['num_comparisons'] = and_comparisons_no_skip

            output_dict['daatAndSkip'][query.strip()] = {}
            output_dict['daatAndSkip'][query.strip()]['results'] = and_op_no_score_skip
            output_dict['daatAndSkip'][query.strip()]['num_docs'] = and_results_cnt_skip
            output_dict['daatAndSkip'][query.strip()]['num_comparisons'] = and_comparisons_skip

            output_dict['daatAndTfIdf'][query.strip()] = {}
            output_dict['daatAndTfIdf'][query.strip()]['results'] = and_op_no_score_no_skip_sorted
            output_dict['daatAndTfIdf'][query.strip()]['num_docs'] = and_results_cnt_no_skip_sorted
            output_dict['daatAndTfIdf'][query.strip()]['num_comparisons'] = and_comparisons_no_skip_sorted

            output_dict['daatAndSkipTfIdf'][query.strip()] = {}
            output_dict['daatAndSkipTfIdf'][query.strip()]['results'] = and_op_no_score_skip_sorted
            output_dict['daatAndSkipTfIdf'][query.strip()]['num_docs'] = and_results_cnt_skip_sorted
            output_dict['daatAndSkipTfIdf'][query.strip()]['num_comparisons'] = and_comparisons_skip_sorted

        return output_dict


@app.route("/execute_query", methods=['POST'])
def execute_query():
    """ This function handles the POST request to your endpoint.
        Do NOT change it."""
    start_time = time.time()

    queries = request.json["queries"]
    random_command = request.json["random_command"]

    """ Running the queries against the pre-loaded index. """
    output_dict = runner.run_queries(queries, random_command)

    """ Dumping the results to a JSON file. """
    with open(output_location, 'w') as fp:
        json.dump(output_dict, fp)

    response = {
        "Response": output_dict,
        "time_taken": str(time.time() - start_time),
        "username_hash": username_hash
    }
    return flask.jsonify(response)

# @app.route('/')
# def hello():
#     return "Hello, World!"


if __name__ == "__main__":
    """ Driver code for the project, which defines the global variables.
        Do NOT change it."""

    output_location = "project2_output.json"
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--corpus", type=str, help="Corpus File name, with path.")
    parser.add_argument("--output_location", type=str, help="Output file name.", default=output_location)
    parser.add_argument("--username", type=str,
                        help="Your UB username. It's the part of your UB email id before the @buffalo.edu. "
                             "DO NOT pass incorrect value here")

    argv = parser.parse_args()

    corpus = argv.corpus
    output_location = argv.output_location
    username_hash = hashlib.md5(argv.username.encode()).hexdigest()

    """ Initialize the project runner"""
    runner = ProjectRunner()

    """ Index the documents from beforehand. When the API endpoint is hit, queries are run against 
        this pre-loaded in memory index. """
    runner.run_indexer(corpus)

    app.run(host="0.0.0.0", port=9999)

# if __name__ == "__main__":
#     """ Initialize the project runner"""
#     runner = ProjectRunner()
#     runner.run_indexer('data/input_corpus.txt')
#     output_location = 'data/ak_output.txt'
#     username_hash = hashlib.md5('annkonna'.encode()).hexdigest()
#     queries = ["the novel coronavirus", "from an epidemic to a pandemic", "is hydroxychloroquine effective?"]
#     # queries = ["recent year"]
#     print(runner.run_queries(queries, "test"))
#     # app.run(host='0.0.0.0', port=9999) #, debug=True)