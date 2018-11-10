#!/usr/bin/python
#weighting.py

import sys,os
import jsonTools as jt
import parseAndInsert as pai
import textAnalyzer as ta
import core
from mongoBall import mongoBall
from stopwords import stopwords
from sparkSetup import sparkSetup

from bson.objectid import ObjectId
import json


def grabWeightingQueryFormat(mdb_collectionType="incidents"):
    """ Determines the query format that the recommendation engine will use for
        weighting based on the collection/ticket type
    Args:
        mdb_collectionType (str):       the type of ticket being searched and recommended against
    Returns:
        weighting_query_format (dict):  format of the mongo query for a specific ticket type
    """
    #determine the mongodb query format based on the collection/ticket type
    if mdb_collectionType == "incidents":
        weight_query_format = {"category1":1, "category2":1, "category3":1, "prodCategory1":1, "prodCategory2":1, "prodCategory3":1, "status":1, "priority":1, "workNotes.summary":1, "workNotes.details":1, "users.name":1, "users.group":1, "reportedDate":1, "resolutionDate":1, "createdDate":1, "closedDate":1, "lastModified":1, "sortedPriority":1,"summary":1,"details":1}
    elif mdb_collectionType == "changes":
        pass
    elif mdb_collectionType == "problems":
        pass
    elif mdb_collectionType == "cic":
        pass
    elif mdb_collectionType == "cir":
        pass
    elif mdb_collectionType == "cis":
        pass
    elif mdb_collectionType == "people":
        pass
    elif mdb_collectionType == "groups":
        pass
    elif mdb_collectionType == "misc":
        pass
    else:
        pass

    return weight_query_format



def getWeightScore(inputSub,mdb_collection,weight_query_format,sc):

    weight_fields_query = mdb_collection.find({},weight_query_format)
    weight_query_text = jt.getText(weight_fields_query)
    weight_queryRDD = sc.parallelize(weight_query_text)

    docRecToToken = core.tokenizeRDD(inputSub,sc)
    queryToToken = core.tokenizeRDD(weight_queryRDD,sc)

    corpus = docRecToToken + queryToToken

    idfsCorpusWeightsBroadcast = core.idfsRDD(corpus,sc)
    docWeightsRDD,docWeightsBroadcast = core.tfidfRDD(docRecToToken,idfsCorpusWeightsBroadcast,sc)
    queryWeightsRDD,queryWeightsBroadcast = core.tfidfRDD(queryToToken,idfsCorpusWeightsBroadcast,sc)

    docNormsBroadcast = core.normalizeRDD(docWeightsRDD,sc)
    queryNormsBroadcast = core.normalizeRDD(queryWeightsRDD,sc)

    docInvPairsRDD = core.invertRDD(docWeightsRDD,sc)
    queryInvPairsRDD = core.invertRDD(queryWeightsRDD,sc)

    commonTokens = core.commonTokensRDD(docInvPairsRDD,queryInvPairsRDD,sc)

    similaritiesRDD = core.cosineSimilarityRDD(commonTokens,docWeightsBroadcast,
                            queryWeightsBroadcast,docNormsBroadcast,queryNormsBroadcast,sc)

    return similaritiesRDD,sorted(similaritiesRDD.collect(), key=lambda similarity: similarity[1])

"""
Joining the weighting and the recommendations RDDs

>>> grouped_rec_weight = similarities_recRDD.join(similarities_weightRDD)

this gives us an RDD containing a list of ((docId,queryId),(rec_score,weight_score))

want to map it to > ((docId,queryId),(rec_score * (1 + weight_score))

>>> final_score = grouped_rec_weight.map(lambda rec: weighting.applyWeights(rec)).cache()

get the list

>>> final_recommendations = sorted(final_score.collect(), key=lambda similarity: similarity[1])
"""

def applyWeights(record):
    docId = record[0][0]
    queryId = record[0][1]
    rec_score = record[1][0]
    weight_score = record[1][1]

    key = (docId, queryId)
    value = rec_score * (1 + weight_score)

    return (key, value)
