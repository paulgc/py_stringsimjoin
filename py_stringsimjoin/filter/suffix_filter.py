import pandas as pd

from math import ceil

class SuffixFilter:

    def __init__(self, token_ordering):
        self.token_ordering = token_ordering
        self.total_tokens = len(self.token_ordering.keys())
        self.max_depth = 2

    def apply_filter(self, l_tokens, r_tokens, l_num_tokens, r_num_tokens, threshold):
        i = 0
        r_token_dict = {}
        for token in r_tokens:
	    r_token_dict[token] = i
	    i += 1
        i = 0
        j = -1
        for token in l_tokens:
            if r_token_dict.get(token) != None:
                j = r_token_dict[token]
                break	
            i += 1

        if j != -1:	
            hamming_dist_max = l_num_tokens + r_num_tokens - 2 * ceil((threshold/(1 + threshold)) * (l_num_tokens + r_num_tokens)) - (i + j - 2)
            hamming_dist = self.suffix_filter(l_tokens[i+1:l_num_tokens], r_tokens[j+1:r_num_tokens], 
                                              l_num_tokens - i - 1, r_num_tokens - j - 1, hamming_dist_max, 1)
            if hamming_dist <= hamming_dist_max:
                return True
        return False

    def suffix_filter(self, l_suffix, r_suffix, l_suffix_num_tokens, r_suffix_num_tokens, hamming_dist_max, depth):
        abs_diff = abs(l_suffix_num_tokens - r_suffix_num_tokens)
        if (depth > self.max_depth) or (l_suffix_num_tokens == 0) or (r_suffix_num_tokens == 0):
            return abs_diff
        mid = int(ceil(r_suffix_num_tokens / 2))
        w = r_suffix[mid]
        o = (hamming_dist_max - abs_diff) / 2
        if l_suffix_num_tokens <= r_suffix_num_tokens:
            o_l = 1
            o_r = 0
        else:
            o_l = 0
            o_r = 1
        (r_l, r_r, f, diff) = self.partition(r_suffix, w, mid, mid)
        (l_l, l_r, f, diff) = self.partition(l_suffix, w, max(0, int(mid - o - abs_diff * o_l)), min(l_suffix_num_tokens - 1, int(mid + o + abs_diff * o_r)))
        r_l_num_tokens = len(r_l)
        r_r_num_tokens = len(r_r)
        l_l_num_tokens = len(l_l)
        l_r_num_tokens = len(l_r)
        hamming_dist = abs(l_l_num_tokens - r_l_num_tokens) + abs(l_r_num_tokens - r_r_num_tokens) + diff
        if hamming_dist > hamming_dist_max:
            return hamming_dist
        else:
            hamming_dist_l = self.suffix_filter(l_l, r_l, l_l_num_tokens, r_l_num_tokens, 
                                                hamming_dist_max - abs(l_r_num_tokens - r_r_num_tokens) - diff, depth + 1)
            hamming_dist = hamming_dist_l + abs(l_r_num_tokens - r_r_num_tokens) + diff
            if hamming_dist <= hamming_dist_max:
                hamming_dist_r = self.suffix_filter(l_r, r_r, l_r_num_tokens, r_r_num_tokens,
                                                    hamming_dist_max - hamming_dist_l - diff, depth + 1)
                return hamming_dist_l + hamming_dist_r + diff
            else:
                return hamming_dist

    def partition(self, tokens, probe_token, l, r):
        if (r < l) or (self.token_ordering.get(tokens[l], self.total_tokens) > self.token_ordering.get(probe_token, self.total_tokens)) or (self.token_ordering.get(tokens[r], self.total_tokens) < self.token_ordering.get(probe_token, self.total_tokens)):
            return ([], [], 0, 1)
        p = self.binary_search(tokens, self.token_ordering.get(probe_token, self.total_tokens), l, r)
        tokens_l = tokens[0:p]
        if tokens[p] == probe_token:
            tokens_r = tokens[p+1:len(tokens)]
            diff = 0
        else:
            tokens_r = tokens[p:len(tokens)]
            diff = 1
        return (tokens_l, tokens_r, 1, diff)

    def binary_search(self, tokens, probe_order, l, r):        
        mid = int(ceil((l+r)/2))
        mid_order = self.token_ordering.get(tokens[mid], self.total_tokens)
        if (l == r) or (mid_order == probe_order):
            return mid
        elif mid_order < probe_order:
            return self.binary_search(tokens, probe_order, mid + 1, r)
        else:
            return self.binary_search(tokens, probe_order, l, mid) 
