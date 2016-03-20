import os
import unittest
import pandas as pd

from py_stringsimjoin.match.match import sim_match, jaccard_match
from py_stringsimjoin.utils.tokenizer import get_delim_tokenizer, get_qgram_tokenizer, tokenize_table
from py_stringsimjoin.utils.token_ordering import gen_token_ordering, order_using_token_ordering
from py_stringsimjoin.utils.sim_utils import get_jaccard_fn
from py_stringsimjoin.filter.size_filter import SizeFilter
from py_stringsimjoin.filter.prefix_filter import PrefixFilter
from py_stringsimjoin.filter.position_filter import PositionFilter
from py_stringsimjoin.filter.suffix_filter import SuffixFilter

table_A = pd.read_csv(os.path.join(os.path.dirname(__file__), 'test_data/songs/table_A.csv'))
table_B = pd.read_csv(os.path.join(os.path.dirname(__file__), 'test_data/songs/table_B.csv'))
l_attr = 'title'
r_attr = 'title'
#tok = get_qgram_tokenizer(3)
tok = get_delim_tokenizer(' ')
tokenized_table_A = tokenize_table(table_A, l_attr, tok)
tokenized_table_B = tokenize_table(table_B, l_attr, tok)
token_ordering = gen_token_ordering(table_A, l_attr)

def compare_matches(m1, m2):
    m1_dict = {}
    for id, row in m1.iterrows():
        m1_dict[str(row['ltable.id'])+','+str(row['rtable.id'])] = True
    for id, row in m2.iterrows():
        if m1_dict.get(str(row['ltable.id'])+','+str(row['rtable.id'])) == None:
            return False
    return True
 
# test cases for jaccard
class JaccardTestCase(unittest.TestCase):
    def setUp(self):
        self.threshold = 0.3
        self.matches_using_cart_prod = sim_match(table_A, table_B, tokenized_table_A, tokenized_table_B, l_attr, r_attr, get_jaccard_fn(), self.threshold, ['id'], ['id'])
        self.size_filter = SizeFilter(table_A, tokenized_table_A, l_attr, tok)
        self.size_filter.build_index()
        self.prefix_filter = PrefixFilter(table_A, tokenized_table_A, l_attr, tok, self.threshold, token_ordering)
        self.prefix_filter.build_index()
        self.position_filter = PositionFilter(table_A, tokenized_table_A, l_attr, tok, self.threshold, token_ordering)
        self.position_filter.build_index()
        self.suffix_filter = SuffixFilter(table_A, tokenized_table_A, l_attr, tok, self.threshold, token_ordering)

    def test_jaccard_match(self):
        # test jaccard with position filter, size filter, suffix filter
        matches = jaccard_match(table_A, table_B, tokenized_table_A, tokenized_table_B, l_attr, r_attr, self.threshold, 
                                [self.position_filter, self.size_filter, self.suffix_filter], ['id'], ['id'])
        self.assertTrue(compare_matches(self.matches_using_cart_prod, matches))        

        # test jaccard with prefix filter, size filter, suffix filter
        matches = jaccard_match(table_A, table_B, tokenized_table_A, tokenized_table_B, l_attr, r_attr, self.threshold,
                                [self.prefix_filter, self.size_filter, self.suffix_filter], ['id'], ['id'])
        self.assertTrue(compare_matches(self.matches_using_cart_prod, matches))

