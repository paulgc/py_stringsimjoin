import pandas as pd

from helper_functions import *

def tokenize_table(table, attr, tokenizer):
    tokenized_table = pd.DataFrame.copy(table)
    tokenized_table[attr] = table[attr].apply(lambda value : set(tokenizer(str(value).lower())))
    return tokenized_table

def get_qgram_tokenizer(q):
    """
    Get a qgram tokenizer that can be called with just input string as the argument

    Parameters
    ----------
    q : integer
        q-value

    Returns
    -------
    tokenizer function
    """

    def tok_qgram(s):
        # check if the input is of type base string
        if pd.isnull(s):
            return s
        if not isinstance(s, basestring):
            raise ValueError('Input should be of type string')
        if q <= 0:
            raise ValueError('q value must be greater than 0')
        return ngrams(s, q)
    return tok_qgram

def get_delim_tokenizer(delim):
    """
    Get a delimiter tokenizer that can be called with just input string as the argument
    
    Parameters
    ----------
    delim : character
            delimiter character used to split strings
    
    Returns
    -------
    tokenizer function
    """

    def tok_delim(s):
        # check if the input is of type base string
        if pd.isnull(s):
            return s
        if not isinstance(s, basestring):
            raise ValueError('Input should be of type string')
        s = remove_non_ascii(s)
        return s.split(delim)
    return tok_delim
