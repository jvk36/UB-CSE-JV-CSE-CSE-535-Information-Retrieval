'''
@author: Sougata Saha
Institute: University at Buffalo
'''

import math


class Node:

    def __init__(self, value=None, next=None, token_count=0):
        """ Class to define the structure of each node in a linked list (postings list).
            Value: document id, Next: Pointer to the next node
            Add more parameters if needed.
            Hint: You may want to define skip pointers & appropriate score calculation here"""
        self.value = value
        self.next = next
        self.skip = None
        self.token_count = token_count
        self.token_freq = 1
        self.tf_idf_score = 0.0


class LinkedList:
    """ Class to define a linked list (postings list). Each element in the linked list is of the type 'Node'
        Each term in the inverted index has an associated linked list object.
        Feel free to add additional functions to this class."""
    def __init__(self):
        self.start_node = None
        self.end_node = None
        self.length, self.n_skips, self.idf = 0, 0, 0.0
        self.skip_length = None

    def add_tf_idf(self, num_docs):
        self.idf = num_docs / self.length
        cur_node = self.start_node
        while cur_node is not None:
            cur_node.tf_idf_score = (cur_node.token_freq / cur_node.token_count) * self.idf
            cur_node = cur_node.next

    def node_pos(self, index):
        cur_node = self.start_node
        for i in range(index):
            cur_node = cur_node.next
        return cur_node

    def swap_nodes(self, index):
        cur_node = self.node_pos(index)
        next_node = self.node_pos(index+1)
        if cur_node == self.start_node:
            self.start_node = next_node
            cur_node.next = next_node.next
            next_node.next = cur_node
        elif next_node == self.end_node:
            prev_node = self.node_pos(index-1)
            self.end_node = cur_node
            cur_node.next = None
            next_node.next = cur_node
            prev_node.next = next_node
        else:
            prev_node = self.node_pos(index-1)
            prev_node.next = next_node
            cur_node.next = next_node.next
            next_node.next = cur_node

    def sort_by_tfidf(self):
        n = self.length
        for i in range(n):
            for j in range(n - i - 1):
                if self.node_pos(j).tf_idf_score < self.node_pos(j + 1).tf_idf_score:
                     self.swap_nodes(j)

    def traverse_list(self):
        traversal = []
        if self.start_node is None:
            return []
        else:
            """ Write logic to traverse the linked list.
                To be implemented."""
            node = self.start_node
            while node is not None:
                traversal.append(node.value)
                node = node.next
            return traversal

    def traverse_skips(self):
        traversal = []
        if self.start_node is None or self.start_node.skip is None:
            return []
        else:
            """ Write logic to traverse the linked list using skip pointers.
                To be implemented."""
            node = self.start_node
            while node is not None:
                traversal.append(node.value)
                node = node.skip
            return traversal

    def add_skip_connections(self):
        self.n_skips = math.floor(math.sqrt(self.length))
        self.skip_length = int(round(math.sqrt(self.length), 0))
        if self.n_skips * self.n_skips == self.length:
            self.n_skips = self.n_skips - 1
        """ Write logic to add skip pointers to the linked list. 
            This function does not return anything.
            To be implemented."""
        if self.n_skips >= 2:
            cur_node = self.start_node
            for i in range(self.n_skips):
                dest_node = cur_node
                for j in range(self.skip_length):
                    dest_node = dest_node.next
                cur_node.skip = dest_node
                cur_node = dest_node


    def insert_at_end(self, value, token_count=0):
        """ Write logic to add new elements to the linked list.
            Insert the element at an appropriate position, such that elements to the left are lower than the inserted
            element, and elements to the right are greater than the inserted element.
            To be implemented. """
        if self.start_node is None:
            self.start_node = self.end_node = Node(value, token_count=token_count)
            self.length = self.length + 1
        elif self.start_node == self.end_node:
            if self.start_node.value == value:
                self.start_node.token_freq += 1
            elif self.start_node.value > value:
                # insert before
                self.start_node = Node(value, token_count=token_count)
                self.length = self.length + 1
                self.start_node.next = self.end_node
            else:
                # insert after
                self.end_node = Node(value, token_count=token_count)
                self.length = self.length + 1
                self.start_node.next = self.end_node
        else:
            # atleast two nodes
            if self.start_node.value == value:
                self.start_node.token_freq += 1
            elif self.end_node.value == value:
                self.end_node.token_freq += 1
            elif self.start_node.value > value:
                # insert at the beginning
                temp_node = self.start_node
                self.start_node = Node(value, token_count=token_count)
                self.length = self.length + 1
                self.start_node.next = temp_node
            elif self.end_node.value < value:
                # insert at the end
                self.end_node.next = Node(value, token_count=token_count)
                self.length = self.length + 1
                self.end_node = self.end_node.next
            else:
                # insert in the middle
                cur_node = self.start_node
                while cur_node is not None:                
                    next_node = cur_node.next
                    if next_node.value == value:
                        next_node.token_freq += 1
                        break
                    if next_node.value < value:
                        cur_node = next_node
                        continue
                    else:
                        cur_node.next = Node(value, token_count=token_count)
                        self.length = self.length + 1
                        cur_node.next.next = next_node
                        break


    def append(self, node_):
        new_node = Node(value=node_.value, token_count=node_.token_count)
        new_node.tf_idf_score = node_.tf_idf_score
        new_node.token_freq = node_.token_freq
        if self.start_node is None:
            self.start_node = self.end_node = new_node
        elif self.start_node == self.end_node:
            self.end_node = new_node
            self.start_node.next = self.end_node
        else:
            self.end_node.next = new_node
            self.end_node = new_node
        self.length += 1


if __name__ == "__main__":
    test_list = LinkedList()
    test_list.insert_at_end(7, 8)
    test_list.add_skip_connections()
    print(test_list.traverse_list())
    print(test_list.traverse_skips())
    test_list.insert_at_end(12, 14)
    test_list.add_skip_connections()
    print(test_list.traverse_list())
    print(test_list.traverse_skips())
    test_list.insert_at_end(9, 13)
    test_list.add_skip_connections()
    print(test_list.traverse_list())
    print(test_list.traverse_skips())
    test_list.insert_at_end(15, 6)
    test_list.add_skip_connections()
    print(test_list.traverse_list())
    print(test_list.traverse_skips())
    test_list.insert_at_end(14, 11)
    test_list.add_skip_connections()
    print(test_list.traverse_list())
    print(test_list.traverse_skips())
    test_list.insert_at_end(13, 17)
    test_list.add_skip_connections()
    print(test_list.traverse_list())
    print(test_list.traverse_skips())
    test_list.insert_at_end(20, 17)
    test_list.add_skip_connections()
    print(test_list.traverse_list())
    print(test_list.traverse_skips())
    test_list.insert_at_end(25, 17)
    test_list.add_skip_connections()
    print(test_list.traverse_list())
    print(test_list.traverse_skips())
    test_list.insert_at_end(22, 17)
    test_list.add_skip_connections()
    print(test_list.traverse_list())
    print(test_list.traverse_skips())
    test_list.add_tf_idf(9)
    cur_node = test_list.start_node
    print(cur_node.value)
    while cur_node is not None:
        print(cur_node.tf_idf_score)
        cur_node = cur_node.next

