import pandas as pd
from math import ceil

from py_stringsimjoin.utils.token_ordering import order_using_token_ordering

class PrefixFilter:

    def __init__(self, table, id, attr, tokenizer, threshold, token_ordering):
        self.table = table
        self.id = id
        self.attr = attr
        self.tokenizer = tokenizer
        self.threshold = threshold
        self.token_ordering = token_ordering
        self.prefix_index = {}

    def build_index(self):
        for row in self.table.itertuples():
            token_list = list(row[self.attr])
            ordered_token_list = order_using_token_ordering(token_list, self.token_ordering)
            num_tokens = len(ordered_token_list)
            prefix_length = int(num_tokens - ceil(self.threshold * num_tokens) + 1)
            i = 0
            for token in ordered_token_list:
                if i == prefix_length:
                    break
                if self.prefix_index.get(token) == None:
                    self.prefix_index[token] = []
                self.prefix_index[token].append(row[self.id])

    def find_candidates(self, probe_tokens, num_tokens, threshold):
        candidates = set()
        prefix_length = int(num_tokens - ceil(threshold * num_tokens) + 1)
        i = 0
        for token in probe_tokens:
            if i == prefix_length:
                break
            cands_for_token = self.prefix_index.get(token)
            if cands_for_token != None:
                for cand in cands_for_token:
                    candidates.add(cand)

        return candidates

    def apply_filter(self, l_tokens, r_tokens, l_num_tokens, r_num_tokens, threshold):
        l_prefix_length = int(l_num_tokens - ceil(threshold * l_num_tokens) + 1)
        r_prefix_length = int(r_num_tokens - ceil(threshold * r_num_tokens) + 1)      
        l_prefix = {}
        i = 0
        for token in l_tokens:
            if i == l_prefix_length:
                break
            l_prefix[token] = True
            i += 1
        i = 0
        for token in r_tokens:
            if i == r_prefix_length:
                break
            if l_prefix.get(token) != None:
                return True
            i += 1
        return False
