
# remove non-ascii characters from string
def remove_non_ascii(s):
    s = ''.join(i for i in s if ord(i) < 128)
    s = str(s)
    return str.strip(s)

def ngrams(input, n):
    output = []
    input = list(input)
    for i in range(len(input)-n+1):
        g = ''.join(input[i:i+n])
        output.append(g)
    return output
