import pandas as pd
from collections import OrderedDict
from math import ceil
import pyprind

from py_stringsimjoin.filter.position_filter import PositionFilter
from py_stringsimjoin.filter.suffix_filter import SuffixFilter
from py_stringsimjoin.utils.token_ordering import gen_token_ordering, order_using_token_ordering
from py_stringsimjoin.utils.tokenizer import tokenize_table
from py_stringsimjoin.utils.sim_utils import get_jaccard_fn
from py_stringsimjoin.filter.filter_utils import analyze_filters, apply_index_filters, apply_index_filters1, apply_non_index_filters

def jaccard_match_opt(ltable, rtable, l_id, l_attr, r_id, r_attr, tokenizer, threshold, prefix_scheme, ltable_output_attrs=None, rtable_output_attrs=None):
    matches_list = []
    sim_function = get_jaccard_fn()
    token_ordering = gen_token_ordering(ltable, l_attr)
    position_filter = PositionFilter(ltable, l_id, l_attr, tokenizer, threshold, token_ordering, prefix_scheme)
    position_filter.build_index()
    suffix_filter = SuffixFilter(token_ordering)

    l_ordered_tokens = {}
    prog_bar = pyprind.ProgBar(len(rtable.index))

    l_dict = {}
    for idx, l_row in ltable.iterrows():
        id = l_row[l_id]
        l_dict[id] = l_row
        l_ordered_tokens[id] = order_using_token_ordering(list(l_row[l_attr]), token_ordering)

    r_dict = {}
    for idx, r_row in rtable.iterrows():
        id = r_row[r_id]
        r_dict[id] = r_row

    for rid in r_dict.keys():
        r_row = r_dict[rid]
        r_tokens = order_using_token_ordering(list(r_row[r_attr]), token_ordering)
        r_num_tokens = len(r_tokens)

        l_cand_ids = position_filter.find_candidates1(r_tokens, r_num_tokens, threshold)
        for lid in l_cand_ids:
            l_row = l_dict[lid]
            l_tokens = l_ordered_tokens[lid]
            if suffix_filter.apply_filter(l_tokens, r_tokens, len(l_tokens), r_num_tokens, threshold):
                if sim_function(l_row[l_attr], r_row[r_attr]) >= threshold:
                    match_dict = get_output_attributes(l_row, r_row, l_id, r_id, ltable_output_attrs, rtable_output_attrs)
                    matches_list.append(match_dict)
                   # matches_list.append(str(lid)+','+str(rid))
        prog_bar.update()

    output_matches = pd.DataFrame(matches_list)
    return output_matches
#    return matches_list

def jaccard_match_adapt(ltable, rtable, l_id, l_attr, r_id, r_attr, tokenizer, threshold, ltable_output_attrs=None, rtable_output_attrs=None):
    matches_list = []
    sim_function = get_jaccard_fn()
    token_ordering = gen_token_ordering(ltable, l_attr)
    position_filter = PositionFilter(ltable, l_id, l_attr, tokenizer, threshold, token_ordering)
    position_filter.build_index()
    suffix_filter = SuffixFilter(token_ordering)

    l_ordered_tokens = {}
    prog_bar = pyprind.ProgBar(len(rtable.index))

    l_dict = {}
    for idx, l_row in ltable.iterrows():
        id = l_row[l_id]
        l_dict[id] = l_row
        l_ordered_tokens[id] = order_using_token_ordering(list(l_row[l_attr]), token_ordering)

    r_dict = {}
    for idx, r_row in rtable.iterrows():
        id = r_row[r_id]
        r_dict[id] = r_row
    c=0
    for rid in r_dict.keys():
        r_row = r_dict[rid]
        r_tokens = order_using_token_ordering(list(r_row[r_attr]), token_ordering)
        r_num_tokens = len(r_tokens)

        l_cand_ids = position_filter.find_candidates1(r_tokens, r_num_tokens, threshold)
        for lid in l_cand_ids:
            l_row = l_dict[lid]
#            l_tokens = l_ordered_tokens[lid]
#            if suffix_filter.apply_filter(l_tokens, r_tokens, len(l_tokens), r_num_tokens, threshold):
            c += 1
            if sim_function(l_row[l_attr], r_row[r_attr]) >= threshold:
                match_dict = get_output_attributes(l_row, r_row, l_id, r_id, ltable_output_attrs, rtable_output_attrs)
                matches_list.append(match_dict)
                  #  matches_list.append(str(lid)+','+str(rid))
        prog_bar.update()
    print c
    output_matches = pd.DataFrame(matches_list)
    return output_matches
#    return matches_list


def sim_match(ltable, rtable, l_id_attr, l_join_attr, r_id_attr, r_join_attr, sim_function, threshold, ltable_output_attrs=None, rtable_output_attrs=None):

    matches_list = []

    l_dict = {}
    for l_id, l_row in ltable.iterrows():
        l_dict[l_id] = l_row

    prog_bar = pyprind.ProgBar(len(rtable.index))

    for r_id, r_row in rtable.iterrows():
        for l_id in l_dict.keys():
            l_row = l_dict[l_id]
            if sim_function(l_row[l_join_attr], r_row[r_join_attr]) >= threshold:
                match_dict = get_output_attributes(l_row, r_row, l_id_attr, r_id_attr, ltable_output_attrs, rtable_output_attrs)
                matches_list.append(match_dict)
        prog_bar.update()

    output_matches = pd.DataFrame(matches_list)
    return output_matches


def get_output_attributes(l_row, r_row, l_id_attr, r_id_attr, ltable_output_attrs=None, rtable_output_attrs=None):
    match_dict = OrderedDict()

    # add ltable id attr
    match_dict['ltable.'+l_id_attr] = l_row[l_id_attr]

    # add ltable output attributes
    if ltable_output_attrs:
        for l_attr in ltable_output_attrs:
            match_dict['ltable.'+l_attr] = l_row[l_attr]

    # add rtable id attr
    match_dict['rtable.'+r_id_attr] = r_row[r_id_attr]

    # add rtable output attributes
    if rtable_output_attrs:
        for r_attr in rtable_output_attrs:
            match_dict['rtable.'+r_attr] = r_row[r_attr]

    return match_dict

def verify_jaccard(r_tokens, l_tokens, threshold, prev_overlap, token_ordering):
    l_num_tokens = len(l_tokens)
    r_num_tokens = len(r_tokens) 
    l_prefix_length = int(l_num_tokens - ceil(threshold * l_num_tokens) + 1)
    r_prefix_length = int(r_num_tokens - ceil(threshold * r_num_tokens) + 1)
    l_last_prefix_token = l_tokens[l_prefix_length - 1]
    r_last_prefix_token = r_tokens[r_prefix_length - 1]
    overlap = prev_overlap
    overlap_threshold = int(ceil((threshold/(1 + threshold)) * (l_num_tokens + r_num_tokens)))
    if token_ordering[r_last_prefix_token] < token_ordering[l_last_prefix_token]:
        upper_bound = prev_overlap + r_num_tokens - r_prefix_length
        if upper_bound >= overlap_threshold:
            overlap += len(set(r_tokens[r_prefix_length:]) & set(l_tokens[prev_overlap:]))
    else:
        upper_bound = prev_overlap + l_num_tokens - l_prefix_length
        if upper_bound >= overlap_threshold:
            overlap += len(set(r_tokens[prev_overlap:]) & set(l_tokens[l_prefix_length:]))   

    if overlap >= overlap_threshold:
        return True
    return False 
