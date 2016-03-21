from math import ceil
from py_stringsimjoin.utils.token_ordering import order_using_token_ordering


class PrefixFilter:

    def __init__(self, table, id_attr, join_attr, tokenizer, threshold, token_ordering, prefix_scheme=1):
        self.table = table
        self.id_attr = id_attr
        self.join_attr = join_attr
        self.tokenizer = tokenizer
        self.threshold = threshold
        self.token_ordering = token_ordering
        self.prefix_index = {}
        self.prefix_scheme = prefix_scheme

    def build_index(self):
        for row in self.table.itertuples():
            token_list = list(row[self.join_attr])
            ordered_token_list = order_using_token_ordering(token_list, self.token_ordering)
            num_tokens = len(ordered_token_list)
            prefix_length = int(num_tokens - ceil(self.threshold * num_tokens) + self.prefix_scheme)
            i = 0
            for token in ordered_token_list:
                if i == prefix_length:
                    break
                if self.prefix_index.get(token) is None:
                    self.prefix_index[token] = []
                self.prefix_index[token].append(row[self.id_attr])

    def find_candidates(self, probe_tokens, num_tokens, threshold):
        candidates = set()
        prefix_length = int(num_tokens - ceil(threshold * num_tokens) + self.prefix_scheme)
        i = 0
        for token in probe_tokens:
            if i == prefix_length:
                break
            candidates_for_token = self.prefix_index.get(token)
            if candidates_for_token is not None:
                for cand in candidates_for_token:
                    candidates.add(cand)

        return candidates

    @staticmethod
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
            if l_prefix.get(token) is not None:
                return True
            i += 1
        return False
