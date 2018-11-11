#!/usr/bin/python
"""
textAnalyzer.py
"""

import re,math

FIELD_PATTERN = '^(\w+)\.*(\w*)'


def removeQuotes(string):
    """ Remove quotation marks from an input string
    Args:
        string (str): input string that might have the quote "" characters
    Returns:
        str: a string without the quote characters
    """

    return ''.join(i for i in string if i != '"')


def parseDatafileLine(datafileLine,datafile_pattern):
    """ Parse a line of the data file using the specified regular
        expression pattern
    Args:
        datafileLine (str): input string that is a line from the data file
    Returns:
        str: a string parsed using the given regular expression and without
             the quote characters
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

def simpleTokenize(string,split_regex=r'\W+'):
    """ A simple implementation of input string tokenization
    Args:
        string (str): input string
    Returns:
        list: a list of tokens
    """

    return filter(lambda string: string != '', re.split(split_regex, string.lower()))

def tokenize(string,stopwords_set):
    """ An implementation of input string tokenization that excludes stopwords
    Args:
        string (str): input string
        stopwords (set): set of stopwords
    Returns:
        list: a list of tokens without stopwords
    """

    return filter(lambda tokens: tokens not in stopwords_set, simpleTokenize(string))

def countTokens(vendorRDD):
    """ Count and return the number of tokens
    Args:
        vendorRDD (RDD of (recordId, tokenizedValue)): Pair tuple of record
                                                       ID to tokenized output
    Returns:
        count: count of all tokens
    """

    return vendorRDD.flatMap(lambda (x,y): y).count()

def termFrequency(tokens):
    """ Compute Term Frequency
    Args:
        tokens (list of str): input list of tokens from tokenize
    Returns:
        dictionary: a dictionary of tokens to its TF values
    """

    tf_dict = {}
    for token in tokens:
        if token in tf_dict:
            tf_dict[token] += 1
        else:
            tf_dict[token] = 1
    tf_norm = {}
    N = len(tokens)
    for key in tf_dict.keys():
        tf_norm[key] = 1. * tf_dict[key] / N
    return tf_norm

def inverseDocumentFrequency(corpus):
    """ Compute Inverse-Document Frequency
    Args:
        corpus (RDD): input corpus
    Returns:
        RDD: a RDD of (token, IDF value)
    """

    N = corpus.map(lambda doc: doc[0]).distinct().count()
    uniqueTokens = corpus.flatMap(lambda doc: list(set(doc[1])))
    tokenCountPairTuple = uniqueTokens.map(lambda token: (token,1))
    tokenSumPairTuple = tokenCountPairTuple.reduceByKey(lambda x,y:x+y)
    return (tokenSumPairTuple.map(lambda (token,count): (token,1. * N / count)))

def tfidf(tokens, idfs):
    """ Compute TF-IDF
    Args:
        tokens (list of str): input list of tokens from tokenize
        idfs (dictionary): record to IDF value
    Returns:
        dictionary: a dictionary of records to TF-IDF values
    """

    tfs = termFrequency(tokens)
    tfidfDict = {k: v*idfs[k] for (k,v) in tfs.items()}
    return tfidfDict

#For cosine similarity
class cosineSimilarity(object):

    def dotprod(self,a,b):
        """ Compute dot product
        Args:
            a (dictionary): first dictionary of record to value
            b (dictionary): second dictionary of record to value
        Returns:
            dotProd: result of the dor product with the two input dictionaries
        """

        return sum([v*b[k] for (k,v) in a.items() if k in b.keys()])

    def norm(self,a):
        """ Compute the square root of the dot product
        Args:
            a (dictionary): a dictionary of record to value
        Returns:
            norm: a dictionary of tokens to its TF values
        """

        return math.sqrt(self.dotprod(a,a))

    def cossim(self,a,b):
        """ Compute cosine similarity
        Args:
            a (dictionary): first dictionary of record to value
            b (dictionary): second dictionary of record to value
        Returns:
            cossim: dot product of two dictionaries divided by the
            norm of the first dictionary and then by the norm of the
            second dictionary
        """

        return self.dotprod(a,b) / (self.norm(a)*self.norm(b))

    def cosineSimilarity(self,string1,string2,idfsDictionary):
        """ Compute cosine similarity between two strings
        Args:
            string1 (str): first string
            string2 (str): second string
            idfsDictionary (dictionary): a dictionary of IDF values
        Returns:
            cossim: cosine similarity value
        """

        w1 = tfidf(tokenize(string1), idfsDictionary)
        w2 = tfidf(tokenize(string2), idfsDictionary)
        return self.cossim(w1,w2)

    def fastCosineSimilarity(self,record,inputWeightsBroadcast,databaseWeightsBroadcast,inputNormsBroadcast,databaseNormsBroadcast):
        """ Compute Cosine Similarity using Broadcast variables
        Args:
            record: ((ID, URL), token)
            inputWeightsBroadcast: broadcasted input TF-IDF weights RDD
            databaseWeightsBroadcast: broadcasted database TF-IDF weights RDD
            inputNormsBroadcast: broadcasted input normalized weights RDD
            databaseNormsBroadcast: broadcasted database normalized weights RDD
        Returns:
            pair: ((ID, URL), cosine similarity value)
        """

        inputRec = record[0][0]
        databaseRec = record[0][1]
        tokens = record[1]
        s = sum([inputWeightsBroadcast.value[inputRec][token] * databaseWeightsBroadcast.value[databaseRec][token] for token in tokens])
        value = s / (dict(inputNormsBroadcast.value)[inputRec] * dict(databaseNormsBroadcast.value)[databaseRec])
        key = (inputRec, databaseRec)
        return (key, value)

def invert(record):
    """ Invert (ID, tokens) to a list of (token, ID)
    Args:
        record: a pair, (ID, token vector)
    Returns:
        pairs: a list of pairs of token to ID
    """

    id_url = record[0]
    weights = record[1]
    pairs = [(token, id_url) for (token, weight) in weights.items()]
    return (pairs)

def swap(record):
    """ Swap (token, (ID,URL)) to ((ID,URL), token)
    Args:
        record: a pair, (token, (ID, URL))
    Returns:
        pair: ((ID, URL), token)
    """

    token = record[0]
    keys = record[1]
    return (keys, token)

def grab_field_content(jsonDoc,field):
    """
    Args:
        mdb_collection: a connection to a mongoDB collection
        field (str): the field to be collected
    Returns:
        pair: ((_id,field_content),1)
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
