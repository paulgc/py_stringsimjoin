import pandas as pd
from math import ceil, floor
from random import randint
from py_stringsimjoin.utils.token_ordering import order_using_token_ordering
from py_stringsimjoin.filter.suffix_filter import SuffixFilter

class PositionFilter:

    def __init__(self, table, id, attr, tokenizer, threshold, token_ordering):
        self.table = table
        self.id = id
        self.attr = attr
        self.tokenizer = tokenizer
        self.threshold = threshold
        self.token_ordering = token_ordering
        self.position_index = []
        self.size_map = {}
        self.min_len = 1000
        self.max_len = 0
        self.avg_len = 0
        self.store = {}

    def build_index(self):
        self.position_index.append({})
        self.position_index.append({})
        num_delta_indexes = 1 
        for idx, row in self.table.iterrows():
            id = row[self.id]
            token_list = list(row[self.attr])
            ordered_token_list = order_using_token_ordering(token_list, self.token_ordering)
            num_tokens = len(ordered_token_list)
            if num_tokens < self.min_len:
                self.min_len = num_tokens
            if num_tokens > self.max_len:
                self.max_len = num_tokens
            self.size_map[id] = num_tokens
            t = ceil(self.threshold * num_tokens)
            one_prefix_length = int(num_tokens - t + 1)
            i = 0
            for i in xrange(0, one_prefix_length):
                if self.position_index[1].get(ordered_token_list[i]) == None:
                    self.position_index[1][ordered_token_list[i]] = []
                self.position_index[1][ordered_token_list[i]].append((id, i))
            j = 2
            for i in xrange(one_prefix_length, num_tokens):
                if j>t:
	            break
                if num_delta_indexes < j:
                    self.position_index.append({})
                    num_delta_indexes += 1
                if self.position_index[j].get(ordered_token_list[i]) == None:
                    self.position_index[j][ordered_token_list[i]] = []
                self.position_index[j][ordered_token_list[i]].append((id, i))
                j += 1

        self.avg_len = (self.min_len + self.max_len) / 2
        n = 0
        x = 1
        for i in xrange(0, 1024):
            if i > x:
                n += 1
                x = 2*x
            self.store[i] = n

    def find_candidates(self, probe_tokens, num_tokens, threshold):
        candidates = set()
        cand_overlap = {}
        prefix_length = int(num_tokens - ceil(self.threshold * num_tokens) + 1)
        i = 0
        for token in probe_tokens:
            if i == prefix_length:
                break
            cands_for_token = self.position_index.get(token)
            if cands_for_token != None:
                for (id, pos) in cands_for_token:
                    cand_num_tokens = self.size_map[id]
                    overlap_threshold = int(ceil((threshold/(1 + threshold)) * (num_tokens + cand_num_tokens)))
                    overlap_upper_bound = 1 + min(num_tokens - i - 1, cand_num_tokens - pos - 1)
                    if cand_overlap.get(id) == None:
                        cand_overlap[id] = 0
                    if (cand_overlap[id] + overlap_upper_bound) >= overlap_threshold:
                        cand_overlap[id] += 1
                    else:
                        cand_overlap[id] = 0
    
        for id in cand_overlap.keys():
            if cand_overlap[id] > 0:
                candidates.add(id)

        return candidates

    def find_candidates2(self, probe_tokens, num_tokens, threshold):

        cand_overlap = {}
        lb = threshold * num_tokens
        ub = num_tokens/threshold
        t = int(ceil(self.threshold * num_tokens))
        one_prefix_length = int(num_tokens - t + 1)
        prefix_scheme= 1
        prev_cand_set_card = 0
        for j in xrange(0, one_prefix_length):
            cands_for_token = self.position_index[1].get(probe_tokens[j])
            if cands_for_token != None:
               for (id, pos) in cands_for_token:
                    cand_num_tokens = self.size_map[id]                   
                    if cand_num_tokens >= lb and cand_num_tokens <= ub:
                        overlap_threshold = int(ceil((threshold/(1 + threshold)) * (num_tokens + cand_num_tokens)))
                        overlap_upper_bound = 1 + min(num_tokens - j - 1, cand_num_tokens - pos - 1)
                        if (cand_overlap.get(id,0) + overlap_upper_bound) >= overlap_threshold:
                            cand_overlap[id] = cand_overlap.get(id, 0) + 1
                            if cand_overlap[id] == 2:
                                prev_cand_set_card += 1
        cost = num_tokens + self.avg_len
        prev_filter_cost = len(cand_overlap.keys())
        prev_total_cost = prev_filter_cost + prev_filter_cost * cost
        K = 10
      
        for i in xrange(2, t+1):
            lists = []
            list_lengths = []
            list_pos = []
            merged_size = 0
            prev_index = -1
            for j in xrange(0, one_prefix_length + i - 1):
                cands_for_token = self.position_index[i].get(probe_tokens[j])
                if cands_for_token != None:
                    lists.append(cands_for_token)
                    list_pos.append(j)
                    list_len = len(cands_for_token)
                    list_lengths.append(list_len if prev_index==-1 else list_len + list_lengths[prev_index])
                    prev_index += 1
                    merged_size += list_len
            for j in xrange(1, i):
                cands_for_token = self.position_index[j].get(probe_tokens[one_prefix_length + i - 2])
                if cands_for_token != None:
                    lists.append(cands_for_token)
                    list_pos.append(one_prefix_length + i - 2)
                    list_len = len(cands_for_token)
                    list_lengths.append(list_len if prev_index==-1 else list_len + list_lengths[prev_index])
                    prev_index += 1
                    merged_size += list_len

            cnt = 0            
            for m in xrange(0, K):
                r = randint(0, merged_size - 1)
                cand_index = 0
                list_index = 0
                for n in xrange(0, prev_index + 1):
                    if r < list_lengths[n]:
                        cand_index = (r if n==0 else r - list_lengths[n -1])
                        list_index = n    
                        break
                if cand_overlap.get(lists[list_index][cand_index][0], 0) == i-1:
                    cnt += 1
            curr_cand_est = prev_cand_set_card  + (1 / K) * cnt * merged_size
            curr_filter_cost = prev_filter_cost + merged_size
            curr_total_cost = curr_filter_cost + curr_cand_est*cost
            if curr_total_cost > prev_total_cost:
                break 
            prefix_scheme = i
            prev_total_cost = curr_total_cost
            prev_filter_cost = curr_filter_cost
            prev_cand_set_card = 0
            l=0
            for list in lists:
                for (id, pos) in list:
                    cand_num_tokens = self.size_map[id]
                    if cand_num_tokens >= lb and cand_num_tokens <= ub:
                        overlap_threshold = int(ceil((threshold/(1 + threshold)) * (num_tokens + cand_num_tokens)))
                        overlap_upper_bound = 1 + min(num_tokens - list_pos[l] - 1, cand_num_tokens - pos - 1)
                        if (cand_overlap.get(id,0) + overlap_upper_bound) >= overlap_threshold:
                            cand_overlap[id] = cand_overlap.get(id, 0) + 1
                            if cand_overlap[id] == i+1:
                                prev_cand_set_card += 1
                l += 1
        candidates = set()        
        for id in cand_overlap.keys():
            if cand_overlap[id] >= prefix_scheme:
                candidates.add(id)
                        
        return candidates

    def find_candidates5(self, probe_tokens, num_tokens, threshold):

        cand_overlap = {}
        overlap_threshold_cache = {}
        lb = int(floor(threshold * num_tokens))
        ub = int(ceil(num_tokens/threshold))
        for i in xrange(lb, ub+1):
            overlap_threshold_cache[i] = int(ceil((threshold/(1 + threshold)) * (num_tokens + i)))
        t = int(ceil(self.threshold * num_tokens))
        one_prefix_length = int(num_tokens - t + 1)
        prefix_scheme= 1
        prev_cand_set_card = 0
        for j in xrange(0, one_prefix_length):
            cands_for_token = self.position_index[1].get(probe_tokens[j])
            if cands_for_token != None:
               for (id, pos) in cands_for_token:
                    cand_num_tokens = self.size_map[id]
                    if cand_num_tokens >= lb and cand_num_tokens <= ub:
                        c = cand_overlap.get(id, 0)
                        if c==0:
                            if num_tokens - j < overlap_threshold_cache[cand_num_tokens] or cand_num_tokens - pos < overlap_threshold_cache[cand_num_tokens]:
                                continue
                        cand_overlap[id] = cand_overlap.get(id, 0) + 1
                        if c+1 == 2:
                            prev_cand_set_card += 1
        cost = num_tokens + self.avg_len
        prev_filter_cost = len(cand_overlap.keys())
        prev_total_cost = prev_filter_cost + prev_filter_cost * cost
        K = 5

        for i in xrange(2, t+1):
            lists = []
            list_lengths = []
            merged_size = 0
            prev_index = -1
            for j in xrange(0, one_prefix_length + i - 1):
                cands_for_token = self.position_index[i].get(probe_tokens[j])
                if cands_for_token != None:
                    lists.append(cands_for_token)
                    list_len = len(cands_for_token)
                    list_lengths.append(list_len if prev_index==-1 else list_len + list_lengths[prev_index])
                    prev_index += 1
                    merged_size += list_len
            for j in xrange(1, i):
                cands_for_token = self.position_index[j].get(probe_tokens[one_prefix_length + i - 2])
                if cands_for_token != None:
                    lists.append(cands_for_token)
                    list_len = len(cands_for_token)
                    list_lengths.append(list_len if prev_index==-1 else list_len + list_lengths[prev_index])
                    prev_index += 1
                    merged_size += list_len

            cnt = 0
            for m in xrange(0, K):
                r = randint(0, merged_size - 1)
                cand_index = 0
                list_index = 0
                for n in xrange(0, prev_index + 1):
                    if r < list_lengths[n]:
                        cand_index = (r if n==0 else r - list_lengths[n -1])
                        list_index = n
                        break
                if cand_overlap.get(lists[list_index][cand_index][0], 0) == i-1:
                    cnt += 1
            curr_cand_est = prev_cand_set_card  + (1 / K) * cnt * merged_size
            curr_filter_cost = prev_filter_cost + merged_size
            curr_total_cost = curr_filter_cost + curr_cand_est*cost
            if curr_total_cost > prev_total_cost:
                break 
            prefix_scheme = i
            prev_total_cost = curr_total_cost
            prev_filter_cost = curr_filter_cost
            prev_cand_set_card = 0
            for list in lists:
                for (id, pos) in list:
                    cand_num_tokens = self.size_map[id]
                    if cand_num_tokens >= lb and cand_num_tokens <= ub:
                        cand_overlap[id] = cand_overlap.get(id, 0) + 1
                        if cand_overlap[id] == i+1:
                            prev_cand_set_card += 1
        candidates = set()
        for id in cand_overlap.keys():
            if cand_overlap[id] >= prefix_scheme:
                candidates.add(id)

        return candidates

    def estimate_cost(self, prefix_size, prefix_scheme, sample_size):
        added_token_list_num = prefix_size + prefix_scheme - 1
        est_cand_cost = added_token_list_num + sample_size*(10 if added_token_list_num >= 1024 else self.store[added_token_list_num])
        est_filter_cost = added_token_list_num
        return est_filter_cost + est_cand_cost
 
    def find_candidates1(self, probe_tokens, num_tokens, threshold):
         
        cand_overlap = {}
        cands = {}
        for i in xrange(1, num_tokens+1):
            cands[i] = []
        overlap_threshold_cache = {}
        lb = int(floor(threshold * num_tokens))
        ub = int(ceil(num_tokens/threshold))
        for i in xrange(lb, ub+1):
            overlap_threshold_cache[i] = int(ceil((threshold/(1 + threshold)) * (num_tokens + i)))
        t = int(ceil(self.threshold * num_tokens))
        one_prefix_length = int(num_tokens - t + 1)
        prefix_scheme= 1
        for j in xrange(0, one_prefix_length):
            cands_for_token = self.position_index[1].get(probe_tokens[j])
            if cands_for_token != None:
               for (id, pos) in cands_for_token:
                    cand_num_tokens = self.size_map[id]
                    if cand_num_tokens >= lb and cand_num_tokens <= ub:
                        c = cand_overlap.get(id, 0)
                        if c==0:
                            if num_tokens - j < overlap_threshold_cache[cand_num_tokens] or cand_num_tokens - pos < overlap_threshold_cache[cand_num_tokens]:
                                continue
                        cand_overlap[id] = cand_overlap.get(id, 0) + 1
                        cands[c+1].append(id)
        unit_cost = 0.5*num_tokens
        K = 5

        for i in xrange(2, t+1):
            cands_prev = len(cands[i-1])
            cands_curr = len(cands[i])
            cost = unit_cost*(cands_prev - cands_curr) - self.estimate_cost(one_prefix_length+i, i+1, K)
            if cost < 0:
                break
            lists = []
            list_lengths = []
            merged_size = 0
            prev_index = -1
            for j in xrange(0, one_prefix_length + i - 1):
                cands_for_token = self.position_index[i].get(probe_tokens[j])
                if cands_for_token != None:
                    lists.append(cands_for_token)
                    list_len = len(cands_for_token)
                    list_lengths.append(list_len if prev_index==-1 else list_len + list_lengths[prev_index])
                    prev_index += 1
                    merged_size += list_len
            for j in xrange(1, i):
                cands_for_token = self.position_index[j].get(probe_tokens[one_prefix_length + i - 2])
                if cands_for_token != None:
                    lists.append(cands_for_token)
                    list_len = len(cands_for_token)
                    list_lengths.append(list_len if prev_index==-1 else list_len + list_lengths[prev_index])
                    prev_index += 1
                    merged_size += list_len
            est_cost = self.estimate_cost(one_prefix_length + i, i + 1, K)
            if unit_cost*cands_prev < merged_size + est_cost:
                break
            cnt = 0
            for m in xrange(0, K):
                r = randint(0, merged_size - 1)
                cand_index = 0
                list_index = 0
                for n in xrange(0, prev_index + 1):
                    if r < list_lengths[n]:
                        cand_index = (r if n==0 else r - list_lengths[n -1])
                        list_index = n
                        break
                if cand_overlap.get(lists[list_index][cand_index][0], 0) == i-1:
                    cnt += 1
            est_cand_size = cands_curr  + (1 / K) * cnt * merged_size
            if unit_cost*cands_prev < unit_cost*est_cand_size + merged_size + est_cost:
                break
            prefix_scheme = i
            for list in lists:
                for (id, pos) in list:
                    cand_num_tokens = self.size_map[id]
                    if cand_num_tokens >= lb and cand_num_tokens <= ub:
                        c = cand_overlap.get(id, 0)
                        if c >= i - 1:
                            cand_overlap[id] += 1
                            cands[c+1].append(id) 

        return cands[prefix_scheme]


    def apply_filter(self, l_tokens, r_tokens, l_num_tokens, r_num_tokens, threshold):
        overlap_threshold = int(ceil((threshold/(1 + threshold)) * (l_num_tokens + r_num_tokens)))
        l_prefix_length = int(l_num_tokens - ceil(threshold * l_num_tokens) + 1)
        r_prefix_length = int(r_num_tokens - ceil(threshold * r_num_tokens) + 1)
        l_prefix = {}
        i = 0
        for token in l_tokens:
            if i == l_prefix_length:
                break
            l_prefix[token] = i
            i += 1
        i = 0
        prev_overlap = 0
        for token in r_tokens:
            if i == r_prefix_length:
                break
            l_pos = l_prefix.get(token)
            if l_pos != None:
                overlap_upper_bound = 1 + min(l_num_tokens - l_pos - 1, r_num_tokens - i - 1)
                if (prev_overlap + overlap_upper_bound) < overlap_threshold:
                    return False
                prev_overlap += 1
            i += 1
        if prev_overlap > 0:
            return True
        return False
