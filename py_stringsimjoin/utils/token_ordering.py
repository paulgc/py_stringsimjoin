
def gen_token_ordering(tokenized_table, attr):
    token_frequency_dict = {}

    for value in tokenized_table[attr]:
        for token in value:
            token_frequency_dict[token] = token_frequency_dict.get(token, 0) + 1    

    token_ordering = {}
    order_idx = 1
    for token in sorted(token_frequency_dict, key=token_frequency_dict.get):
        token_ordering[token] = order_idx
        order_idx += 1

    return token_ordering

def order_using_token_ordering(tokens, token_ordering):
    if not isinstance(tokens, list):
        tokens = list(tokens)
    tokens_dict = {}
    new_tokens = []
    for token in tokens:
        order_index = token_ordering.get(token)
        if order_index != None:
            tokens_dict[token] = order_index
        else:
            new_tokens.append(token)
    ordered_tokens = []
    for token in sorted(tokens_dict, key=tokens_dict.get):
        ordered_tokens.append(token)
    for token in new_tokens:
        ordered_tokens.append(token)
    
    return ordered_tokens
