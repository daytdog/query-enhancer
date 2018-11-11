#!/user/bin/python
"""
textAnalyzer.py
"""

import re,sys,os
import match

FIELD_PATTERN = '^(\w+)\.*(\w*)'

def removeQuotes(string):
    """
    Removes quotation marks from an input string
    """

    return ''.join(ch for ch in string if ch != '"')


def parseDatafileLine(datafileLine, datafile_pattern):
    """
    Parse a line of the data file using the specified
    regular expression pattern (is this needed?)
    """

    match = re.search(datafile_pattern, datafileLine)
    if match is None:
        print 'Invalid datafile line: %s' % datafileLine
        return (datafileLine, -1)
    elif match.group(1) == '"name"':
        print 'Header datafile line: %s' % datafileLine
    else:
        product = '%s %s' % (match.group(1), match.group(2))
        return (product, 1)


def simpleTokenize(string, split_regex=r'\W+'):
    """
    A simple implementation of input string tokenization
    """

    return filter(lambda string: string != '', re.split(split_regex, string.lower()))


def tokenize(string,stopwords_set):
    """
    An implementation of input string tokenization that excludes stopwords
    """

    return filter(lambda tokens: tokens not in stopwords_set, simpleTokenize(string))


#probably not needed
def countTokens(vendorRDD):
    """
    Count and return the number of tokens
    """

    return vendorRDD.flatMap(lambda (x,y): y).count()


def termFrequency(tokens):
    """
    Compute Term Frequency of the tokens for each video item
    """

    tf_dict = {}
    for token in tokens:
        if token in tf_dict:
            tf_dict[token] += 1
        else:
            tf_dict[token] = 1
    return tf_dict


#For cosine similarity
class cosineSimilarity(object):

    def dotprod(self,a,b):
        """
        Compute dot product
        """

        return sum([v*b[k] for (k,v) in a.items() if k in b.keys()])

    def norm(self,a):
        """
        Compute the square root of the dot product
        """

        return math.sqrt(self.dotprod(a,a))

    def cossim(self,a,b):
        """
        Compute cosine similarity
        """

        return self.dotprod(a,b) / (self.norm(a)*self.norm(b))

    def cosineSimilarity(self,string1,string2,idfsDictionary):
        """
        Compute cosine similarity between two strings
        """

        w1 = tfidf(tokenize(string1), idfsDictionary)
        w2 = tfidf(tokenize(string2), idfsDictionary)
        return self.cossim(w1,w2)

def invert(record):
    """
    Invert (ID, tokens) to a list of (token, ID)
    """

    id_url = record[0]
    weights = record[1]
    pairs = [(token, id_url) for (token, weight) in weights.items()]
    return (pairs)

def swap(record):
    """
    Swap (token, (ID,URL)) to ((ID,URL), token)
    """

    token = record[0]
    keys = record[1]
    return (keys, token)

def grab_field_content(jsonDoc,field):
    """
    """

    match = re.search(FIELD_PATTERN, field)
    is_base_field = False
    if match.group(2) == '':
        is_base_field = True

    if is_base_field == True:
        print jsonDoc[match.group(1)]
    else:
        for thing in jsonDoc[match.group(1)]:
            print thing[match.group(2)]
