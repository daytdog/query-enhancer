#!/usr/bin/python
#preProcess.py

#packages from rec_engine
from rec_engine import corpus as cor
from rec_engine import jsonTextParse as jtp
from rec_engine import jsonTools as jt
from rec_engine import parseAndInsert as pai
from rec_engine import textAnalyzer as ta
from rec_engine import core
from rec_engine import weighting
from rec_engine.mongoBall import mongoBall,recConfig,recConfig_weights,mongoURIBall
from rec_engine.stopwords import stopwords

#packages outside of rec_engine
import sys,os
from pyspark import SparkConf, SparkContext
from bson.objectid import ObjectId
import json

"""
preProcess.py is the core application that is submitted to spark-submit
for the relevancy engine. This application is called in the following way:

    ~$ preProcess.py groupingId <mongoDB_uri> <mongoDB_collection> <recConfig_uri> <recConfig_collection>

"""

APP_NAME = "Relevancy Engine Pre-Process"

def usage():
    print __doc__
    sys.exit()


def grabWeightingQuery(mdb_collection,mdb_collectionType="incidents",groupingID="538f9e119e16abd715913ab5"):
    #grab the query format (default to "incidents" type messages)
    weight_query_format = weighting.grabWeightingQueryFormat(mdb_collectionType)

    #get a handle on this query from the mongoDB collection
    weight_fields_query = mdb_collection.find({'groupingId':"538f9e119e16abd715913ab5"},weight_query_format)

    return weight_fields_query

def generateFieldValuesForParameters(weight_fields_query):
    #this will grab the values of the fields querried

    summariesCountRDD = tokenizeFieldCount("summary",mdb_collection,sc,sw_set)
    detailsCountRDD = tokenizeFieldCount("details",mdb_collection,sc,sw_set)
    fieldValuesForParameters = summariesCountRDD.join(detailsCountRDD).groupByKey()

def tokenizeFieldCount(field_name,mdb_collection,sc,sw_set):
    field_query = mdb_collection.find({},{field_name:1})
    field_query_text = jt.getText(field_query)
    field_queryRDD = sc.parallelize(field_query_text)

    field_queryToToken = core.tokenizeRDD(field_queryRDD,sc,sw_set)
    counted_field_tokensRDD = field_queryToToken.map(lambda rec: (rec[0],(field_name,len(rec[1]))))

    return counted_field_tokensRDD

def run_preProcessing():
    pass

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
    #if at least 1 argument isn't given, display usage
    if (len(sys.argv)<2): usage()

    #get the groupingId initial input argument
    #(testing is being done on "538f9e119e16abd715913ab5")
    #(Enigma Analytics groupingId is '5708279864a89c6f1b5bd28f')
    groupingId = unicode(sys.argv[1])

    #<mongoURL>
    #"mongodb://atlasuser:atlasuserpw#1@10.132.154.147:27017/atlas"
    mdb_uri = "mongodb://plasmuser:plasmpass@10.10.10.243:27017/plasm"
    if (len(sys.argv)>2): mdb_uri=str(sys.argv[2])

    #<mdb_message_collection_name>
    #"message"
    mdb_collection_name = "message"
    if (len(sys.argv)>3): mdb_collection_name=str(sys.argv[3])

    #create the mongoBall and get the collection connector
    mdb = mongoURIBall(mdb_uri)
    mdb_collection = mdb.collection(mdb_collection_name)
    print "connecting to the %s collection" % (mdb_collection_name)


    #<recConfig_uri>
    recConfig_uri = "mongodb://plasmuser:plasmpass@10.10.10.243:27017/plasm"
    if (len(sys.argv)>4): recConfig_uri=str(sys.argv[4])

    #<recConfig_collection>
    recConfig_collection = "recConfig"
    if (len(sys.argv)>5): recConfig_collection=str(sys.argv[5])

    #get the recConfig collection connector
    recConfig_handle = recConfigURI(mongoURI=recConfig_uri,collection=recConfig_collection,groupingId=groupingId)
    recConfig_collection = recConfig_handle.collection
    print "connecting to the %s recConfig collection" % (recConfig_collection)

    #with all of the required variables gathered, run the engine
    run_preProcessing(groupingId,mdb_collection,recConfig_collection,sw_set,sc)

    #objectId of an interesting ticket, will be useful for programming the components:
    #{u'_id': ObjectId('570bf4a064a8f86db80b32ee')}


if __name__ == "__main__":
    #configure Spark
    conf = SparkConf()

    #set app name for Spark
    conf = conf.setAppName(APP_NAME)

    #get the Spark Context
    sc = SparkContext(conf=conf)

    #set up the stopwords list
    sw = stopwords(sc)
    sw_set = sw.stopwords_set

    #execute main functionality
    main(sc,sw,sw_set)
