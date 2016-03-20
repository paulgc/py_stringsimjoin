
from py_stringsimjoin.filter.size_filter import SizeFilter
from py_stringsimjoin.filter.prefix_filter import PrefixFilter
from py_stringsimjoin.filter.position_filter import PositionFilter
from py_stringsimjoin.filter.suffix_filter import SuffixFilter

def analyze_filters(filters):
    index_filters = []
    non_index_filters = []
    token_ordering = {}
    need_ordering_for_index_filters = False
    need_ordering_for_non_index_filters = False
    for filter in filters:
        if isinstance(filter, SizeFilter):
            if filter.size_index:
                index_filters.append(filter)
            else:
                non_index_filters.append(filter)
        elif isinstance(filter, PrefixFilter):
            token_ordering = filter.token_ordering
            if filter.prefix_index:
                index_filters.append(filter)
                need_ordering_for_index_filters = True
            else:
                non_index_filters.append(filter)
                need_ordering_for_non_index_filters = True
        elif isinstance(filter, PositionFilter):
            token_ordering = filter.token_ordering
            if filter.position_index:
                index_filters.append(filter)
                need_ordering_for_index_filters = True
            else:
                non_index_filters.append(filter)
                need_ordering_for_non_index_filters = True
        elif isinstance(filter, SuffixFilter):
            token_ordering = filter.token_ordering
            non_index_filters.append(filter)
            need_ordering_for_non_index_filters = True

    return (index_filters, non_index_filters, token_ordering, need_ordering_for_index_filters, need_ordering_for_non_index_filters)

def apply_index_filters(probe_tokens, r_num_tokens, threshold, index_filters):
    candidates = set()
    for filter in index_filters:
        curr_cands = filter.find_candidates(probe_tokens, r_num_tokens, threshold)
        if candidates:
            candidates.intersection_update(curr_cands)
        else:
            candidates = curr_cands

    return candidates

def apply_index_filters1(probe_tokens, threshold, index_filters, prefix_overlap):
    candidates = set()
    for filter in index_filters:
        if isinstance(filter, PositionFilter):
            curr_cands = filter.find_candidates1(probe_tokens, threshold, prefix_overlap)
        else:
            curr_cands = filter.find_candidates(probe_tokens, threshold)
        if candidates:
            candidates.intersection_update(curr_cands)
        else:
            candidates = curr_cands

    return candidates

def apply_non_index_filters(l_tokens, r_tokens, l_num_tokens, r_num_tokens, threshold, non_index_filters):
    for filter in non_index_filters:
        if not filter.apply_filter(l_tokens, r_tokens, l_num_tokens, r_num_tokens, threshold):
            return False
    return True

