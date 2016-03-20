import unittest
import pandas as pd

from py_stringsimjoin.utils.tokenizer import get_delim_tokenizer, tokenize_table
from py_stringsimjoin.utils.token_ordering import gen_token_ordering, order_using_token_ordering
from py_stringsimjoin.utils.sim_utils import get_jaccard_fn
from py_stringsimjoin.filter.size_filter import SizeFilter
from py_stringsimjoin.filter.prefix_filter import PrefixFilter
from py_stringsimjoin.filter.position_filter import PositionFilter
from py_stringsimjoin.filter.suffix_filter import SuffixFilter

A = pd.DataFrame([{'id':1, 'str':'ab cd ef aa bb'}, 
                  {'id':2, 'str':''}, 
                  {'id':3, 'str':'ab'},
                  {'id':4, 'str':'aa bb cd ef fg'},
                  {'id':5, 'str':'xy xx zz fg'}])
tok = get_delim_tokenizer(' ')
A_tokenized = tokenize_table(A, 'str', tok)
token_ordering = gen_token_ordering(A_tokenized, 'str')

# test cases for size filter
class SizeFilterTestCase(unittest.TestCase):
    def setUp(self):
        self.size_filter = SizeFilter(A, A_tokenized, 'str', tok)
        self.size_filter.build_index()

    def test_apply_filter(self):
        # size filter satisfies
        l_tokens = ['aa', 'bb', 'cd', 'ef', 'fg']
        r_tokens = ['xx', 'yy', 'aa', 'bb']
        self.assertTrue(self.size_filter.apply_filter(l_tokens, r_tokens, len(l_tokens), len(r_tokens), 0.8))

        # size filter doesn't satisfy
        l_tokens = ['aa', 'bb', 'cd', 'ef', 'fg']
        r_tokens = ['xx']   
        self.assertFalse(self.size_filter.apply_filter(l_tokens, r_tokens, len(l_tokens), len(r_tokens), 0.8))

        # test empty list of tokens
        l_tokens = ['aa', 'bb', 'cd', 'ef', 'fg']
        r_tokens = []
        self.assertFalse(self.size_filter.apply_filter(l_tokens, r_tokens, len(l_tokens), len(r_tokens), 0.8))
        self.assertFalse(self.size_filter.apply_filter(r_tokens, l_tokens, len(r_tokens), len(l_tokens), 0.8))

    def test_find_candidates(self):
        # test default case (presence of candidates)
        tokens = ['aa', 'xx', 'yy', 'uu']
        self.assertSetEqual(self.size_filter.find_candidates(tokens, len(tokens), 0.8), set([0, 3, 4]))

        # test empty set of candidates
        tokens = ['aa', 'op', 'xx', 'yy', 'uu', 'yu', 'iu', 'lp']
        self.assertSetEqual(self.size_filter.find_candidates(tokens, len(tokens), 0.8), set())

        # test empty list of probe tokens
        tokens = []
        self.assertSetEqual(self.size_filter.find_candidates(tokens, len(tokens), 0.8), set())

# test cases for prefix filter
class PrefixFilterTestCase(unittest.TestCase):
    def setUp(self):
        self.prefix_filter = PrefixFilter(A, A_tokenized, 'str', tok, 0.8, token_ordering)
        self.prefix_filter.build_index()

    def test_apply_filter(self):
        # prefix filter satisfies
        l_tokens = order_using_token_ordering(['aa', 'bb', 'cd', 'ef', 'fg'], token_ordering)
        r_tokens = order_using_token_ordering(['fg', 'cd', 'aa'], token_ordering)
        self.assertTrue(self.prefix_filter.apply_filter(l_tokens, r_tokens, len(l_tokens), len(r_tokens), 0.8))

        l_tokens = order_using_token_ordering(['aa', 'bb', 'cd', 'ef', 'fg'], token_ordering)
        r_tokens = order_using_token_ordering(['aa'], token_ordering)
        self.assertTrue(self.prefix_filter.apply_filter(l_tokens, r_tokens, len(l_tokens), len(r_tokens), 0.8))

        # prefix filter doesn't satisfy
        l_tokens = order_using_token_ordering(['aa', 'bb', 'cd', 'ef', 'fg'], token_ordering)
        r_tokens = order_using_token_ordering(['fg'], token_ordering)
        self.assertFalse(self.prefix_filter.apply_filter(l_tokens, r_tokens, len(l_tokens), len(r_tokens), 0.8))

        # test empty list of tokens
        l_tokens = order_using_token_ordering(['aa', 'bb', 'cd', 'ef', 'fg'], token_ordering)
        r_tokens = order_using_token_ordering([], token_ordering)
        self.assertFalse(self.prefix_filter.apply_filter(l_tokens, r_tokens, len(l_tokens), len(r_tokens), 0.8))
        self.assertFalse(self.prefix_filter.apply_filter(r_tokens, l_tokens, len(r_tokens), len(l_tokens), 0.8))
    
    def test_find_candidates(self):
        # test default case (presence of candidates)
        tokens = order_using_token_ordering(['aa', 'ef', 'lp'], token_ordering)
        self.assertSetEqual(self.prefix_filter.find_candidates(tokens, len(tokens), 0.8), set([0, 3]))

        # test empty set of candidates
        tokens = order_using_token_ordering(['op', 'lp', 'mp'], token_ordering)
        self.assertSetEqual(self.prefix_filter.find_candidates(tokens, len(tokens), 0.8), set())

        # test empty list of probe tokens
        tokens = order_using_token_ordering([], token_ordering)
        self.assertSetEqual(self.prefix_filter.find_candidates(tokens, len(tokens), 0.8), set())

# test cases for position filter
class PositionFilterTestCase(unittest.TestCase):
    def setUp(self):
        self.position_filter = PositionFilter(A, A_tokenized, 'str', tok, 0.8, token_ordering)
        self.position_filter.build_index()

    def test_apply_filter(self):
        # position filter satisfies
        l_tokens = order_using_token_ordering(['aa', 'bb', 'cd', 'ef', 'fg'], token_ordering)
        r_tokens = order_using_token_ordering(['fg', 'cd', 'aa', 'ef'], token_ordering)
        self.assertTrue(self.position_filter.apply_filter(l_tokens, r_tokens, len(l_tokens), len(r_tokens), 0.8))

        # position filter doesn't satisfy
        l_tokens = order_using_token_ordering(['aa', 'bb', 'cd', 'ef', 'fg'], token_ordering)
        r_tokens = order_using_token_ordering(['fg'], token_ordering)     
        self.assertFalse(self.position_filter.apply_filter(l_tokens, r_tokens, len(l_tokens), len(r_tokens), 0.8))

        # prefix filter satisfies but position filter doesn't satisfy
        l_tokens = order_using_token_ordering(['aa', 'bb', 'cd', 'ef', 'fg'], token_ordering)
        r_tokens = order_using_token_ordering(['aa'], token_ordering)
        self.assertFalse(self.position_filter.apply_filter(l_tokens, r_tokens, len(l_tokens), len(r_tokens), 0.8))

        # test empty list of tokens
        l_tokens = order_using_token_ordering(['aa', 'bb', 'cd', 'ef', 'fg'], token_ordering)
        r_tokens = order_using_token_ordering([], token_ordering)
        self.assertFalse(self.position_filter.apply_filter(l_tokens, r_tokens, len(l_tokens), len(r_tokens), 0.8))
        self.assertFalse(self.position_filter.apply_filter(r_tokens, l_tokens, len(r_tokens), len(l_tokens), 0.8))

    def test_find_candidates(self):
        # test default case (presence of candidates)
        tokens = order_using_token_ordering(['aa', 'ef', 'ab', 'cd'], token_ordering)
        self.assertSetEqual(self.position_filter.find_candidates(tokens, len(tokens), 0.8), set([0, 3]))
         
        # test empty set of candidates
        tokens = order_using_token_ordering(['op', 'lp', 'mp'], token_ordering)
        self.assertSetEqual(self.position_filter.find_candidates(tokens, len(tokens), 0.8), set())

        # prefix index returns 2 candidates where as position index prunes them
        tokens = order_using_token_ordering(['aa', 'ef', 'lp'], token_ordering)
        self.assertSetEqual(self.position_filter.find_candidates(tokens, len(tokens), 0.8), set())

        # test empty list of probe tokens
        tokens = order_using_token_ordering([], token_ordering)
        self.assertSetEqual(self.position_filter.find_candidates(tokens, len(tokens), 0.8), set())

# test cases for suffix filter
class SuffixFilterTestCase(unittest.TestCase):
    def setUp(self):
        self.suffix_filter = SuffixFilter(A, A_tokenized, 'str', tok, 0.8, token_ordering)

    def test_apply_filter(self):
        # suffix filter satisfies
        l_tokens = order_using_token_ordering(['aa', 'bb', 'cd', 'ef', 'fg'], token_ordering)
        r_tokens = order_using_token_ordering(['fg', 'cd', 'aa'], token_ordering)
        self.assertTrue(self.suffix_filter.apply_filter(l_tokens, r_tokens, len(l_tokens), len(r_tokens), 0.8))

        # suffix filter doesn't satisfy
        l_tokens = order_using_token_ordering(['aa', 'bb', 'cd', 'ef', 'fg'], token_ordering)
        r_tokens = order_using_token_ordering(['fg'], token_ordering)
        self.assertFalse(self.suffix_filter.apply_filter(l_tokens, r_tokens, len(l_tokens), len(r_tokens), 0.8))


        # position filter satisfies but suffix filter doesn't satisfy
        l_tokens = order_using_token_ordering(['aa', 'cd', 'ef', 'fg'], token_ordering)
        r_tokens = order_using_token_ordering(['cd', 'xx', 'xy', 'aa'], token_ordering)
        self.assertFalse(self.suffix_filter.apply_filter(l_tokens, r_tokens, len(l_tokens), len(r_tokens), 0.8))

        # test empty list of tokens
        l_tokens = order_using_token_ordering(['aa', 'bb', 'cd', 'ef', 'fg'], token_ordering)
        r_tokens = order_using_token_ordering([], token_ordering)
        self.assertFalse(self.suffix_filter.apply_filter(l_tokens, r_tokens, len(l_tokens), len(r_tokens), 0.8))
        self.assertFalse(self.suffix_filter.apply_filter(r_tokens, l_tokens, len(r_tokens), len(l_tokens), 0.8))
