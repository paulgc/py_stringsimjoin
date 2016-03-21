import pandas as pd
from math import ceil, floor


class SizeFilter:

    def __init__(self, table, id_attr, join_attr, tokenizer):
        self.table = table
        self.id_attr = id_attr
        self.join_attr = join_attr
        self.tokenizer = tokenizer
        self.size_index = {}

    def build_index(self):
        for row in self.table.itertuples():
            num_tokens = len(row[self.join_attr])
            if self.size_index.get(num_tokens) is None:
                self.size_index[num_tokens] = []
            self.size_index[num_tokens].append(row[self.id_attr])

    def find_candidates(self, probe_tokens, num_tokens, threshold):
        start_length = int(floor(threshold*num_tokens))
        end_length = int(ceil(num_tokens/threshold))
        candidates = set()
        for i in xrange(start_length, end_length+1):
            candidates_for_i = self.size_index.get(i)
            if candidates_for_i is not None:
                for cand in candidates_for_i:
                    candidates.add(cand)
        return candidates

    @staticmethod
    def apply_filter(l_tokens, r_tokens, l_num_tokens, r_num_tokens, threshold):
        if (threshold * l_num_tokens <= r_num_tokens) and (r_num_tokens <= (l_num_tokens / threshold)):
            return True
        return False 
