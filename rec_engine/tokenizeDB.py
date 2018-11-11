#!/usr/bin/python
#tokenizeDB.py

from rec_engine import corpus as cor
from rec_engine import jsonTextParse as jtp
from rec_engine import jsonTools as jt
from rec_engine import parseAndInsert as pai
from rec_engine import textAnalyzer as ta
from rec_engine import core
from rec_engine import weighting
from rec_engine import algorithms
from rec_engine import submissionChecks
from rec_engine.mongoBall import mongoURIBall,recConfigURI
from rec_engine.stopwords import stopwords

#packages outside of rec_engine
import sys,os,json,datetime
from pyspark import SparkConf,SparkContext
from bson.objectid import ObjectId

"""
submission.py is the core application that is submitted to spark-submit
for the recommendation engine. This application is called in the following way:

    ~$ submission.py submissionType inputSub <mongoDB_uri> <groupingId> <mongoDB_collection> <results_collection> <rlvcStats_collection> <message_type> <weightedBool> <recConfig_collection> <recConfig_uri>
"""

APP_NAME = "Tokenization of MongoDB Collection"

def usage():
    print __doc__
    sys.exit()


def run_tokenizeDB(groupingId,mdb_collection,mdb,rlvcStats_collection,recConfig_collection,sw_set,recConfig_handle):
    return



def main(sc,sw,sw_set):
    #if at least 2 arguments aren't given, display usage
    if (len(sys.argv)<3): usage()

    #get the groupingId initial input argument
    #(testing is being done on "538f9e119e16abd715913ab5")
    #(Enigma Analytics groupingId is '5708279864a89c6f1b5bd28f')
    groupingId = unicode(sys.argv[1])

    #<mongoURL>
    #"mongodb://atlasuser:atlasuserpw#1@10.132.154.147:27017/atlas"
    #mdb_uri = "mongodb://plasmuser:plasmpass@10.10.10.243:27017/plasm"
    mdb_uri=str(sys.argv[2])

    #<mdb_message_collection_name>
    #"message"
    mdb_collection_name = "message"
    if (len(sys.argv)>3): mdb_collection_name=str(sys.argv[3])

    #<rlvcStats_collection_name>
    rlvcStats_collection_name = "rlvcStats"
    if (len(sys.argv)>4): rlvcStats_collection_name = str(sys.argv[4])

    #create the mongoBall and get the collection connector
    mdb = mongoURIBall(mdb_uri)
    mdb_collection = mdb.collection(mdb_collection_name)
    print "connecting to the %s collection" % (mdb_collection_name)

    #get the rlvcStats collection connector
    rlvcStats_collection = mdb.collection(rlvcStats_collection_name)
    print "connecting to the %s collection" % (rlvcStats_collection_name)

    #<recConfig_collection>
    recConfig_collection = "recConfig"
    if (len(sys.argv)>5): recConfig_collection=str(sys.argv[5])

    #<recConfig_uri> unless a separate uri is given, it will use the same database as the messages
    recConfig_uri = mdb_uri
    if (len(sys.argv)>6): recConfig_uri=str(sys.argv[6])

    #get the recConfig collection connector
    recConfig_handle = recConfigURI(mongoURI=recConfig_uri,collection=recConfig_collection,groupingId=groupingId)
    recConfig_collection = recConfig_handle.collection
    print "connecting to the %s recConfig collection" % (recConfig_collection)

    #with all of the required variables gathered, run the engine
    #run_preProcessing(groupingId,mdb_collection,mdb,recConfig_collection,sw_set)
    run_tokenizeDB(groupingId,mdb_collection,mdb,rlvcStats_collection,recConfig_collection,sw_set,recConfig_handle)


if __name__ == "__main__":
    #configure Spark
    conf = SparkConf()
    #set app name for Spark
    conf = conf.setAppName(APP_NAME)
    #get the Spark Context
    sc = SparkContext(conf=conf)

    #set up the stopwords list
    sw = stopwords()
    sw_set = sw.stopwords_set

    #execute main functionality
    main(sc,sw,sw_set)
