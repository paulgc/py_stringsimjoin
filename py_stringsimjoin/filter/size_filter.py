import pandas as pd
from math import ceil, floor

class SizeFilter:

    def __init__(self, table, id, attr, tokenizer):
        self.table = table
        self.id = id
        self.attr = attr
        self.tokenizer = tokenizer
        self.size_index = {}

    def build_index(self):
        for row in self.table.itertuples():
            num_tokens = len(row[self.attr])
            if self.size_index.get(num_tokens) == None:
                self.size_index[num_tokens] = []
            self.size_index[num_tokens].append(row[self.id])

    def find_candidates(self, probe_tokens, num_tokens, threshold):
        start_length = int(floor(threshold*num_tokens))
        end_length = int(ceil(num_tokens/threshold))
        candidates = set()
        for i in xrange(start_length, end_length+1):
            cands_for_i = self.size_index.get(i)
            if cands_for_i != None:
                for cand in cands_for_i:
                    candidates.add(cand)
        return candidates

    def apply_filter(self, l_tokens, r_tokens, l_num_tokens, r_num_tokens, threshold):
        if (threshold * l_num_tokens  <= r_num_tokens) and (r_num_tokens <= (l_num_tokens / threshold)):
            return True
        return False 
