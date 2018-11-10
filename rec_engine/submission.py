#!/usr/bin/python
#submission.py

from rec_engine import corpus as cor
from rec_engine import jsonTextParse as jtp
from rec_engine import jsonTools as jt
from rec_engine import parseAndInsert as pai
from rec_engine import textAnalyzer as ta
from rec_engine import core
from rec_engine import weighting
from rec_engine.mongoBall import mongoBall,mongoURIBall
from rec_engine.stopwords import stopwords
#from rec_engine.sparkSetup import sparkSetup

#packages outside of rec_engine
import sys,os
from pyspark import SparkConf, SparkContext
#from bson.objectid import ObjectId
import json

"""
submission.py is the core application that is submitted to spark-submit
for the recommendation engine. This application is called in the following way:

    ~$ submission.py submissionType inputSub <mongoDB_uri> <groupingId> <mongoDB_collection> <message_type> <weightedBool>

    Args:
        submission_type (str):      The type of input submission. This will
                                    be either "file", "single", or "query"

        inputSub (object):          Documents that recommendations are being
                                    requested for. This will be either a json
                                    dictionary, a file name, or a MongoDB query

    (Optional)
        mongoDB_uri (str):          URI of the Mongo database that contains the
                                    documents to be recommended against.
                                    (default: "mongodb://localhost:27017/test")

        groupingId (str):           The groupingId that the input document belongs
                                    to in the Mongo Database.
                                    (default: "5708279864a89c6f1b5bd28f")

        mongoDB_collection (str):   Name of the MongoDB collection that contains
                                    the documents that will be recommended.
                                    (default: "message")

        message_type (str):         Type of messages that are contained in the
                                    MongoDB collection.
                                    (default: "incidents")

        weightedBool (boolean):     Whether or not the recommendation results will
                                    also go through an extra step of weighting
                                    that will compare certain extra fields.
                                    (default: False)
"""

APP_NAME = "Recommendation Engine Submission"

def usage():
    print __doc__
    sys.exit()

def grabQueryFormat(mdb_collectionType="incidents"):
    """ Determines the query format that the recommendation engine will use to grab
        documents from MongoDB based on the collection/ticket type
    Args:
        mdb_collectionType (str):   the type of ticket being searched and recommended against
    Returns:
        query_format (dict):        format of the mongo query for a specific ticket type
    """
    #determine the mongodb query format based on the collection/ticket type
    if mdb_collectionType == "incidents":
        query_format = {"summary":1, "details":1, "workNotes.details":1, "workNotes.summary":1}
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
        query_format = {"summary":1, "details":1}

    return query_format


def setup_file(infile,mdb_collection,query_format,sc):
    """ Sets up variables to pass to the recommend method
    Args:
        infile (str):               the name of the file containing the json documents
        mdb_collection (pymongo collection): a mongoDB collection pointer
        query_format (dict):        format of the mongo query for a specific ticket type
        sc (Spark Context):         Spark Context Environment
    Returns:
        subRDD (RDD):               RDD of a single json document
    """

    #insert the documents into MongoDB:
    insertedIds = pai.paiManyWithCollection(infile,mdb_collection)

    #grab the documents from mongoDB now, and parse them to the query format we're looking for
    mongoDocs = mdb_collection.find({"_id":{"$in":insertedIds}},query_format)

    #concatenate the string fields into a single string field with jsonTools.getText()
    mongoDocs_text = jt.getText(mongoDocs)

    #parallelize the documents into an RDD
    subRDD = sc.parallelize(mongoDocs_text)

    #FOR TESTING PURPOSES TO KEEP THE DATABASE CLEAR OF DUPLICATES
    #mdb_collection.delete_many({"_id":{"$in":insertedIds}})

    return subRDD,insertedIds


def setup_singleDoc(inDoc,mdb_collection,query_format,sc):
    """ Sets up variables to pass to the recommend method
    Args:
        inDoc (dict):               a single json document
        mdb_collection (pymongo collection): a mongoDB collection pointer
        query_format (dict):        format of the mongo query for a specific ticket type
        sc (Spark Context):         Spark Context Environment
    Returns:
        subRDD (RDD):               RDD of a single json document
    """

    #insert the single document into MongoDB to get an _id and allow for easy filtering
    if type(inDoc) == dict:
        inDocId = mdb_collection.insert(inDoc)
    else:
        raise Exception("The inserted document was not of the JSON dictionary format for Python")

    #grab the document from mongoDB now, and parse it to the query format we're looking for
    mongoDoc = mdb_collection.find({"_id":inDocId},query_format)

    #concatenate the string fields into a single string field with jsonTools.getText()
    mongoDoc_text = jt.getText(mongoDoc)

    #parallelize the document into an RDD
    subRDD = sc.parallelize(mongoDoc_text)

    #FOR TESTING PURPOSES TO KEEP THE DATABASE CLEAR OF DUPLICATES
    #mdb_collection.delete_one({"_id":inDocId})
    inDocIdList = [inDocId]

    return subRDD,inDocIdList


def setup_query(idList,mdb_collection,query_format,sc):
    """ Sets up variables to pass to the recommend method
    Args:
        idList (list):              a list of ObjectIds corresponding to the documents in the collection
        mdb_collection (pymongo collection): a mongoDB collection pointer
        query_format (dict):        format of the mongo query for a specific ticket type
        sc (Spark Context):         Spark Context Environment
    Returns:
        subRDD (RDD):               RDD of a single json document
    """

    #grab the documents from mongoDB now, and parse them to the query format we're looking for
    mongoDocs = mdb_collection.find({"_id":{"$in":idList}},query_format)

    #concatenate the string fields into a single string field with jsonTools.getText()
    mongoDocs_text = jt.getText(mongoDocs)

    #parallelize the documents into an RDD
    subRDD = sc.parallelize(mongoDocs_text)

    return subRDD


def recommend(inputSub,mdb_collection,query_format,sw_set,sc):
    """ Recommends which documents in a mongo collection would be most relevent 
        to a submitted list of documents
    Args:
        inputSub (RDD):             RDD containing a list of documents that recommendations are being requested for
        mdb_collection (pymongo collection): a mongoDB collection pointer
        query_format (dict):        format of the mongo query for a specific ticket type
        sw_set (set):               a set of stopwords to be ignored when tokenizing
        sc (Spark Context):         Spark Context Environment
    Returns:
        similaritiesRDD (RDD):      RDD of cosine similarities between the submitted documents and the query documents
        recommendations (list):     an accending sorted list of recommendations for the submitted documents
    """
    #Set up the corpus of the dataset to be recommended against (using incident documents for now)
    mdb_query = mdb_collection.find({},query_format)
    query_text = jt.getText(mdb_query)
    queryRDD = sc.parallelize(query_text)

    #tokenize the input document and remove stopwords
    print "Tokenizing the submitted document and the query"
    docRecToToken = core.tokenizeRDD(inputSub,sc,sw_set)
    docRecToTokenCount = docRecToToken.count()
    print "%i documents found in the submitted document" % docRecToTokenCount

    #tokenize the query and get its inverse-document-frequency with the document
    queryToToken = core.tokenizeRDD(queryRDD,sc,sw_set)
    queryToTokenCount = queryToToken.count()
    print "%i documents found in the query" % queryToTokenCount
    corpus = docRecToToken + queryToToken
    corpusCount = corpus.count()
    print "%i documents found in the corpus" % corpusCount

    #collect the weights of the idfsCorpus and broadcast them
    print "Calculating corpus IDF weights"
    idfsCorpusWeightsBroadcast = core.idfsRDD(corpus,sc)

    #get the weights of the submitted document and of the query and broadcast them
    print "Calculating submitted document and query TF-IDF weights"
    docWeightsRDD,docWeightsBroadcast = core.tfidfRDD(docRecToToken,idfsCorpusWeightsBroadcast,sc)
    queryWeightsRDD,queryWeightsBroadcast = core.tfidfRDD(queryToToken,idfsCorpusWeightsBroadcast,sc)

    #collect the normalized weights of the submitted document and the query and broadcast them
    print "Normalizing the submitted document and query's TF-IDF weights"
    docNormsBroadcast = core.normalizeRDD(docWeightsRDD,sc)
    queryNormsBroadcast = core.normalizeRDD(queryWeightsRDD,sc)

    #invert the (_id,weights) pairs to (weights,_id) pairs
    print "Inverting (_id,weight) pairs of the submitted document and the query to (weight,_id) pairs"
    docInvPairsRDD = core.invertRDD(docWeightsRDD,sc)
    queryInvPairsRDD = core.invertRDD(queryWeightsRDD,sc)

    #print a count of the inverted pairs for both the document and query
    print "docInvPairsRDD count: %i, queryInvPairsRDD count: %i" % (docInvPairsRDD.count(),queryInvPairsRDD.count())

    #collect a list of common tokens between the query and submitted document
    print "Collecting common tokens found between the submitted document and the query"
    commonTokens = core.commonTokensRDD(docInvPairsRDD,queryInvPairsRDD,sc)

    print "commonTokens count: %i" % commonTokens.count()

    #Run Cosine Similarity between the submitted document and the query
    print "Calculating cosine similarity between the submitted document and the query"
    similaritiesRDD = core.cosineSimilarityRDD(commonTokens,docWeightsBroadcast,
                            queryWeightsBroadcast,docNormsBroadcast,queryNormsBroadcast,sc)

    print "Done!"
    #print "Comparisons: %i" % similaritiesRDD.count()

    #return the sorted cosine similarity results and the raw RDD
    return similaritiesRDD,sorted(similaritiesRDD.collect(), key=lambda similarity: similarity[1])

def topNRecommendations(recommendations,n,mdb_collection):
    """
    Args:
        recommendations (list):              a list of recommendations (from recommend())
        n (int):                             the number of top recommendations to be returned
        mdb_collection (pymongo collection): a mongoDB collection pointer
    Returns:
        idList (list):                       a list of ObjectIds of the top N recommendations
        docList (pymongo cursor):            a list of documents returned from querying the
                                             idList against the mongodb collection
    """
    short_rec = recommendations[-n:]
    idList = []
    for rec in short_rec:
        idList.append(rec[0][1])

    mdb_cursor = mdb_collection.find({"_id":{"$in":idList}})

    docList = []
    for doc in mdb_cursor:
        docList.append(doc)

    return idList,docList

def run_engine(submission_type,inputSub,mdb_collection,mdb_collectionType,sw_set,sc,weighted=False):
    """ Configures setup variables based on the submission type and runs the recommendation engine
    Args:
        submission_type (str):      the type of input submission. This will be either "file", "single", or "query"
        inputSub (object):          documents that recommendations are being requested for. This
                                    will be either a json dictionary, a file name, or a mongoDB query
        mdb_collection (pymongo collection): a mongoDB collection pointer
        mdb_collectionType (str):   the type of ticket being searched and recommended against
        sw_set (set):               a set of stopwords to be ignored when tokenizing
        sc (Spark Context):         Spark Context Environment
    Returns:
        similaritiesRDD (RDD):      RDD of cosine similarities between the submitted documents and the query documents
        recommendations (list):     an accending sorted list of recommendations for the submitted documents
    """

    #DEFAULT FOR TESTING PURPOSES
    #mdb_collectionType="incidents"

    #get the format of the mongodb query
    query_format = grabQueryFormat(mdb_collectionType)

    #Configure setup variables based on the submission type
    if submission_type == "file":       #input = multiple json documents from a text file
        subRDD,insertedIds = setup_file(inputSub,mdb_collection,query_format,sc)

    elif submission_type == "single":   #input = a single json document (type: dict)
        subRDD,insertedIds = setup_singleDoc(inputSub,mdb_collection,query_format,sc)

    elif submission_type == "query":    #input = results of a mongo database query

        subRDD = setup_query(inputSub,mdb_collection,query_format,sc)
        insertedIds = None

    else:       #if the submission_type is not one of the three accepted types, raise an error
        raise Exception("submission_type must be one of the following values: \"file\", \"single\", or \"query\"")


    #run the recommendation function with the configured variables
    similarities_recRDD,recommendations = recommend(subRDD,mdb_collection,query_format,sw_set,sc)


    #if using weights
    if weighted == True:
        #get the weighting MongoDB query format
        weight_query_format = weighting.grabWeightingQueryFormat(mdb_collectionType)
        #create the input weighting RDD
        weight_subRDD = setup_query(insertedIds,mdb_collection,weight_query_format,sc)
        #generate the weights
        similarities_weightRDD,weights = weighting.getWeightScore(weight_subRDD,mdb_collection,weight_query_format,sc)

        #group the recommendations and the weights together
        grouped_rec_weight = similarities_recRDD.join(similarities_weightRDD)

        #apply the weights to the recommendations and get the final score
        final_score = grouped_rec_weight.map(lambda rec: weighting.applyWeights(rec)).cache()
        final_recommendations = sorted(final_score.collect(), key=lambda similarity: similarity[1])

        return final_score,final_recommendations,insertedIds

    else:
        #return similarities_recRDD,recommendations,insertedIds
        print recommendations[-20:][:10]
        mdb_collection.delete_many({"_id":{"$in":insertedIds}})
        print mdb_collection.count()

def main(sc,sw,sw_set):
    #if at least 2 arguments aren't given, display usage
    if (len(sys.argv)<3): usage()

    #get first two input arguments
    submission_type = unicode(sys.argv[1])
    inputSub = sys.argv[2]

    #if file name doesn't exist, display usage
    if submission_type == u'file':
        if (os.path.exists(inputSub)==0): usage()

    #<mongoURL>
    #"mongodb://atlasuser:atlasuserpw#1@10.132.154.147:27017/atlas"
    mdb_uri = "mongodb://atlasuser:atlasuserpw#1@dev.puzzlelogic.com:27017/atlas"
    if (len(sys.argv)>3): mdb_uri=str(sys.argv[3])

    #<groupingId>
    #"538f9e119e16abd715913ab5"
    groupingId = "5708279864a89c6f1b5bd28f"
    if (len(sys.argv)>4): groupingId=str(sys.argv[4])

    #<mdb_message_collection_name>
    #"message"
    mdb_collection_name="message"
    if (len(sys.argv)>5): mdb_collection_name=str(sys.argv[5])


    #create the mongoBall and get the collection connector
    mdb = mongoURIBall(mdb_uri)
    mdb_collection = mdb.collection(mdb_collection_name)
    print "connecting to the %s collection" % (mdb_collection_name)

    #get the MongoDB collection ticket type
    mdb_collectionType="incidents"
    if (len(sys.argv)>6): mdb_collectionType=str(sys.argv[6])

    #get whether the results should be weighted or not
    weighted=False
    if (len(sys.argv)>7): weighted=str(sys.argv[7]).lower() in ("yes","true","t","1")

    #with all of the required variables gathered, run the engine
    run_engine(submission_type,inputSub,mdb_collection,mdb_collectionType,sw_set,sc,weighted=weighted)

if __name__ == "__main__":
    #configure Spark
    conf = SparkConf()
    #set app name for Spark
    conf = conf.setAppName(APP_NAME)
    #set master credentials for Spark
    #conf = conf.setMaster("spark://localhost:7077")
    #conf = conf.set("spark.shuffle.manager","SORT")
    #conf = conf.set("spark.shuffle.io.maxRetries", "20")
    #conf = conf.set("spark.sql.broadcastTimeout","300s")
    #conf = conf.set("spark.driver.memory","6g")
    #conf = conf.set("spark.executor.memory","6g")
    #conf = conf.set("spark.cores.max","6")
    #get the Spark Context
    sc = SparkContext(conf=conf)

    #set up the stopwords list
    sw = stopwords(sc)
    sw_set = sw.stopwords_set

    #execute main functionality
    main(sc,sw,sw_set)
