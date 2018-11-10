#!/usr/bin/python
#core.py

import textAnalyzer as ta


def tokenizeRDD(inputRDD,sc,sw_set=None):
    """ Tokenizes an RDD of a list of (_id,string) pairs
    Args:
        inputRDD (RDD):     RDD of a list of (_id,string) pairs to be tokenized
        sc (Spark Context): Spark Context Environment
        sw_set (set):       A set of stopwords to be ignored when tokenizing
    Returns:
        An RDD of a list of tokenized (_id,[tokens]) pairs
    """
    #if the stopword set doesn't exist, run a simple tokenization
    if sw_set == None:
        print "running simple tokenization"
        return inputRDD.map(lambda rec: (rec[0], ta.simpleTokenize(rec[1])))
    #else tokenize and remove stopwords
    else:
        print "running tokenization with stopwords"
        return inputRDD.map(lambda rec: (rec[0], ta.tokenize(rec[1],sw_set)))

def idfsRDD(corpusRDD,sc):
    """ Computes the inverse-document-frequency of a set of tokenized pairs
    Args:
        corpusRDD (RDD):     RDD of tokenized pairs to be run through inverse-document-frequency
        sc (Spark Context): Spark Context environment
    Returns:
        idfsInputWeightsBroadcast (RDD): broadcast of an RDD of inverse-document-frequency weights
    """
    #tokenize the query and get its inverse-document-frequency with the document
    idfsInputRDD = ta.inverseDocumentFrequency(corpusRDD)

    #collect the weights of the idfsCorpus and broadcast them
    idfsInputWeightsBroadcast = sc.broadcast(idfsInputRDD.collectAsMap())

    return idfsInputWeightsBroadcast

def tfidfRDD(tokenizedRDD,idfsInputWeightsBroadcast,sc):
    """ Computes the term-frequency inverse-document-frequency of a tokenized RDD
    Args:
        tokenizedRDD (RDD):              RDD of tokenized pairs to be run through tf-idf (from tokenizeRDD())
        idfsInputWeightsBroadcast (RDD): broadcasted idfs RDD (from idfsRDD())
        sc (Spark Context):              Spark Context environment
    Returns:
        tfidfWeightsRDD (RDD):       RDD of term-frequency inverse-document-frequency weights
        tfidfWeightsBroadcast (RDD): broadcast of tfidfWeightsRDD
    """
    #get the weights of the submitted document and of the query
    tfidfWeightsRDD = tokenizedRDD.map(lambda rec: (rec[0],ta.tfidf(rec[1],idfsInputWeightsBroadcast.value)))

    #broadcast the submitted document and query weights
    tfidfWeightsBroadcast = sc.broadcast(tfidfWeightsRDD.collectAsMap())

    return tfidfWeightsRDD, tfidfWeightsBroadcast

def normalizeRDD(weightsRDD,sc):
    """ Normalize the weights of the terms computed from TF-IDF
    Args:
        weightsRDD (RDD):       RDD of computed TF-IDF weights (from tfidfRDD())
        sc (Spark Context):     Spark Context environment
    Returns:
        normsBroadcast (RDD):   broadcast of an RDD of the normalized TF-IDF weights
    """
    #CHECK IF USING THE BROADCASTED WEIGHTS WOULD WORK HERE! weightsBroadcast RATHER THAN weightsRDD
    #collect the normalized weights of the submitted document and the query and broadcast them
    normsRDD = weightsRDD.map(lambda (_id,weights): (_id, ta.cosineSimilarity().norm(weights)))
    normsBroadcast = sc.broadcast(normsRDD.collect())

    return normsBroadcast

def invertRDD(weightsRDD,sc):
    """ Inverts the (_id,weights) pairs to (weights,_id) pairs
    Args:
        weightsRDD (RDD):       RDD of computed TF-IDF weights (from tfidfRDD())
        sc (Spark Context):     Spark Context environment
    Returns:
        a cached RDD of inverted pairs of the form: (weights,_id)
    """
    #invert the (_id,weights) pairs to (weights,_id) pairs
    return (weightsRDD.flatMap(lambda rec: ta.invert(rec)).cache())

def commonTokensRDD(invertedPair1RDD,invertedPair2RDD,sc):
    """ Collects a list of common tokens between two inverted pair RDDs
    Args:
        invertedPair1RDD (RDD): the first RDD of inverted pairs (from invertRDD())
        invertedPair2RDD (RDD): the second RDD of inverted pairs (from invertRDD())
        sc (Spark Context):     Spark Context environment
    Returns:
        a cached list of shared tokens
    """
    #collect a list of common tokens between the query and submitted document
    return (invertedPair1RDD.join(invertedPair2RDD)
                            .map(lambda rec: ta.swap(rec))
                            .groupByKey()
                            .map(lambda rec: (rec[0], list(rec[1])))
                            .cache())

def cosineSimilarityRDD(commonTokens,weightsBroadcast1,weightsBroadcast2,normsBroadcast1,normsBroadcast2,sc):
    """ Computes the cosine similarity between two TF-IDF computed RDDs
    Args:
        commonTokens (RDD):      a list of common tokens found between two tokenized documents
        weightsBroadcast1 (RDD): first broadcast of computed TF-IDF weights (from tfidfRDD())
        weightsBroadcast2 (RDD): second broadcast of computed TF-IDF weights (from tfidfRDD())
        normsBroadcast1 (RDD):   first broadcast of an RDD of normalized TF-IDF weights (from normalizeRDD())
        normsBroadcast2 (RDD):   second broadcast of an RDD of normalized TF-IDF weights (from normalizeRDD())
        sc (Spark Context):      Spark Context environment
    Returns:
        a cached list of document similarities: ((doc_id1,doc_id2),cosine_similarity)
    """

    #Run Cosine Similarity between the submitted document and the query
    return (commonTokens.map(lambda rec: ta.cosineSimilarity().fastCosineSimilarity(rec,
                                            weightsBroadcast1,weightsBroadcast2,
                                            normsBroadcast1,normsBroadcast2))
                                            .cache())
