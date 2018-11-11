#!/usr/bin/python
#submission.py

from rec_engine import jsonTools as jt
from rec_engine import parseAndInsert as pai
from rec_engine import core
from rec_engine import weighting
from rec_engine import submissionChecks
from rec_engine.mongoBall import mongoURIBall,recConfigURI
from rec_engine.stopwords import stopwords
#from rec_engine.sparkSetup import sparkSetup

#packages outside of rec_engine
import sys,os,datetime
from pyspark import SparkConf,SparkContext
from bson.objectid import ObjectId

"""
submission.py is the core application that is submitted to spark-submit
for the recommendation engine. This application is called in the following way:

    ~$ submission.py submissionType inputSub <mongoDB_uri> <groupingId> <mongoDB_collection> <results_collection> <rlvcStats_collection> <message_type> <weightedBool> <recConfig_collection> <recConfig_uri>

    Args:
        submission_type (str):      The type of input submission. This will
                                    be either "file", "single", or "query"

        inputSub (object):          Documents that recommendations are being
                                    requested for. This will be either a json
                                    dictionary, a file name, or a MongoDB query

    (Optional):
        mongoDB_uri (str):          URI of the Mongo database that contains the
                                    documents to be recommended against.
                                    (default: "mongodb://atlasuser:atlasuserpw#1@dev.puzzlelogic.com:27017/atlas")

        groupingId (str):           The groupingId that the input document belongs
                                    to in the Mongo Database.
                                    (default: '5708279864a89c6f1b5bd28f')

        mongoDB_collection (str):   Name of the MongoDB collection that contains
                                    the documents that will be recommended.
                                    (default: "message")

        results_collection (str):   Name of the MongoDB collection that contains
                                    the results of the recommendation that will
                                    be recommended.
                                    (default: "recResult")

        rlvcStats_collection (str): Name of the MongoDB collection that contains
                                    the statistics of the rlvcScores of the given
                                    groupingId
                                    (default: "rlvcStats")

        mdb_collectionType (str):   Type of messages that are contained in the
                                    MongoDB collection.
                                    (default: "incidents")

        weightedRlvc (boolean):     Whether or not the recommendation engine will
                                    also take into account the rlvcScore of the
                                    documents when calculating the recommendation
                                    score.
                                    (default: True)

        recConfig_collection (str): Name of the MongoDB collection that contains
                                    user-configurable variables for use in both
                                    the recommendation engine and the pre-
                                    processing engine
                                    (default: "recConfig")

        recConfig_uri (str):        URI of the Mongo Database that contains the
                                    user-configurable variables documents
                                    (default: same as mongoDB_uri)
"""

APP_NAME = "Recommendation Engine Submission"

def usage():
    print __doc__
    sys.exit()

#-----------------------------------------------------#
#              Modules for Input Setup                #
#-----------------------------------------------------#
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

def grabQueryFormat(mdb_collectionType="incidents",firstPass=True):
    """ Determines the query format that the recommendation engine will use to grab
        documents from MongoDB based on the collection/ticket type
    Args:
        mdb_collectionType (str):   the type of ticket being searched and recommended against
    Returns:
        query_format (dict):        format of the mongo query for a specific ticket type
    """
    #determine the mongodb query format based on the collection/ticket type
    if mdb_collectionType == "incidents":
        if firstPass == True:
            #query_format = {"summary":1, "details":1, "workNotes.details":1, "workNotes.summary":1}
            #query_format = {"summary":1, "details":1}
            #query_format = {"summary":1, "details":1, "relatedCis":1, "category1":1, "category2":1, "category3":1, "prodCategory1":1, "prodCategory2":1, "prodCategory3":1, "productName":1}
            query_format = {"summary":1, "relatedCis":1, "category1":1, "category2":1, "category3":1, "prodCategory1":1, "prodCategory2":1, "prodCategory3":1, "productName":1}
        elif firstPass == False:
            #query_format = {"summary":1, "details":1, "workNotes.summary":1}
            query_format = {"summary":1, "details":1, "workNotes.summary":1, "workNotes.details":1}

    elif mdb_collectionType == "changes":
        if firstPass == True:
            pass
        elif firstPass == False:
            pass

    elif mdb_collectionType == "problems":
        if firstPass == True:
            pass
        elif firstPass == False:
            pass

    elif mdb_collectionType == "cic":
        if firstPass == True:
            pass
        elif firstPass == False:
            pass

    elif mdb_collectionType == "cir":
        if firstPass == True:
            pass
        elif firstPass == False:
            pass

    elif mdb_collectionType == "cis":
        if firstPass == True:
            pass
        elif firstPass == False:
            pass

    elif mdb_collectionType == "people":
        if firstPass == True:
            pass
        elif firstPass == False:
            pass

    elif mdb_collectionType == "groups":
        if firstPass == True:
            pass
        elif firstPass == False:
            pass

    elif mdb_collectionType == "misc":
        if firstPass == True:
            pass
        elif firstPass == False:
            pass

    else:
        query_format = {"summary":1, "details":1}

    return query_format

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


#-----------------------------------------------------#
#            Modules for Recommendation               #
#-----------------------------------------------------#
def recommend(inputSub,mdb_collection,rlvcStats_collection,groupingId,query_format,sw_set,sc):
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
    #HARDCODED THE LIMIT FOR TESTING PURPOSES, REMOVE LATER

    # -=Filtered Query=-
    #grab the documents from MongoDB and filter the results to use the top N percent
    collectionRlvcScoreStats = rlvcStats_collection.find_one({'groupingId':groupingId},{mdb_collection.name:1})
    cutoffRlvcScore = collectionRlvcScoreStats[mdb_collection.name]['cutoffRlvcScore']
    mdb_query = mdb_collection.find({'groupingId':groupingId,'rlvcScore':{"$gt":cutoffRlvcScore}},query_format)

    #mdb_query = mdb_collection.find({},query_format).limit(100000)
    #mdb_query = mdb_collection.find({'groupingId':groupingId},query_format).limit(10000)
    #mdb_query = mdb_collection.find({'groupingId':groupingId,'rlvcScore':{"$gt":0.57525269274493995}},query_format)
    query_text = jt.getText(mdb_query)
    queryRDD = sc.parallelize(query_text)

    return queryRDD

def getSimilarityScore(subRDD,queryRDD,sw_set,sc):

    #tokenize the input document and remove stopwords
    print "\nTokenizing the submitted document and the query"
    docRecToToken = core.tokenizeRDD(subRDD,sc,sw_set)
    docRecToTokenCount = docRecToToken.count()

    #tokenize the query and get its inverse-document-frequency with the document
    queryToToken = core.tokenizeRDD(queryRDD,sc,sw_set)
    queryToTokenCount = queryToToken.count()
    corpus = docRecToToken + queryToToken
    corpusCount = corpus.count()
    print "Done!\n%i documents found in the submitted document" % docRecToTokenCount
    print "%i documents found in the query" % queryToTokenCount
    print "%i documents found in the corpus" % corpusCount

    #collect the weights of the idfsCorpus and broadcast them
    print "\nCalculating corpus IDF weights"
    idfsCorpusWeightsBroadcast = core.idfsRDD(corpus,sc)
    print "Done!"

    #get the weights of the submitted document and of the query and broadcast them
    print "\nCalculating submitted document and query TF-IDF weights"
    docWeightsRDD,docWeightsBroadcast = core.tfidfRDD(docRecToToken,idfsCorpusWeightsBroadcast,sc)
    queryWeightsRDD,queryWeightsBroadcast = core.tfidfRDD(queryToToken,idfsCorpusWeightsBroadcast,sc)
    print "Done!"

    #collect the normalized weights of the submitted document and the query and broadcast them
    print "\nNormalizing the submitted document and query's TF-IDF weights"
    docNormsBroadcast = core.normalizeRDD(docWeightsRDD,sc)
    queryNormsBroadcast = core.normalizeRDD(queryWeightsRDD,sc)
    print "Done!"

    #invert the (_id,weights) pairs to (weights,_id) pairs
    print "\nInverting (_id,weight) pairs of the submitted document and the query to (weight,_id) pairs"
    docInvPairsRDD = core.invertRDD(docWeightsRDD,sc)
    queryInvPairsRDD = core.invertRDD(queryWeightsRDD,sc)

    #print a count of the inverted pairs for both the document and query
    print "Done!\ndocInvPairsRDD count: %i, queryInvPairsRDD count: %i" % (docInvPairsRDD.count(),queryInvPairsRDD.count())

    #collect a list of common tokens between the query and submitted document
    print "\nCollecting common tokens found between the submitted document and the query"
    commonTokens = core.commonTokensRDD(docInvPairsRDD,queryInvPairsRDD,sc)
    print "Done!\ncommonTokens count: %i" % commonTokens.count()

    #Run Cosine Similarity between the submitted document and the query
    print "\nCalculating cosine similarity between the submitted document and the query"
    similaritiesRDD = core.cosineSimilarityRDD(commonTokens,docWeightsBroadcast,
                            queryWeightsBroadcast,docNormsBroadcast,queryNormsBroadcast,sc)

    #print "Comparisons: %i" % similaritiesRDD.count()

    #return the sorted cosine similarity results and the raw RDD
    return similaritiesRDD,sorted(similaritiesRDD.collect(), key=lambda similarity: similarity[1])

def topNRecommendations(recommendations,n,inputSub):
    """
    Args:
        recommendations (list):              a list of recommendations (from recommend())
        n (int):                             the number of top recommendations to be returned
    Returns:
        resultsList (list):                  a list of ObjectIds of the top N recommendations and their scores
    """
    #get top N results of recommendations (n+1 in case the doc itself is in that list)
    short_rec = recommendations[-(n+1):]
    resultsList = []
    for rec in short_rec:
        #filter out if it matches with itself
        if rec[0] != ObjectId(inputSub):
            dictionary_format = {"outputId":rec[0],"score":rec[1]}
            resultsList.append(dictionary_format)

    #make sure the list only has a length of N
    resultsList = sorted(resultsList[-n:],key=lambda k: k['score'],reverse=True)

    return resultsList

def grabIdList(recommendations):
    """
    Args:
        recommendations (list):              a list of recommendations (from recommend())
    Returns:
        recIdList (list):                  a list of ObjectIds of the returned recommendations
    """
    recIdList = []
    for rec in recommendations:
        recIdList.append(rec[0][1])
    return recIdList

def setupInputSubRDD(inputSub,mdb_collection,query_format,sc,submission_type):
    """Sets up the RDD for the input ObjectId or list of input ObjectIds

    """
    #Configure setup variables based on the submission type
    if submission_type == "file":       #input = multiple json documents from a text file
        subRDD,insertedIds = setup_file(inputSub,mdb_collection,query_format,sc)

    elif submission_type == "single":   #input = a single json document (type: dict)
        subRDD,insertedIds = setup_singleDoc(inputSub,mdb_collection,query_format,sc)

    elif submission_type == "query":    #input = results of a mongo database query
        inputSubList = [ObjectId(inputSub)]
        subRDD = setup_query(inputSubList,mdb_collection,query_format,sc)
        insertedIds = None

    else:       #if the submission_type is not one of the three accepted types, raise an error
        raise Exception("submission_type must be one of the following values: \"file\", \"single\", or \"query\"")

    return subRDD,insertedIds

def run_engine(submission_type,inputSub,groupingId,mdb_collection,mdb_collectionType,sw_set,sc,mdb_results_collection,rlvcStats_collection,recConfig_handle,weighted=False):
    """ Configures setup variables based on the submission type and runs the recommendation engine
    Args:
        submission_type (str):      the type of input submission. This will be either "file", "single", or "query"
        inputSub (object):          documents that recommendations are being requested for. This
                                    will be either a json dictionary, a file name, or a mongoDB query
        mdb_collection (pymongo collection): a mongoDB collection pointer
        mdb_collectionType (str):   the type of ticket being searched and recommended against
        sw_set (set):               a set of stopwords to be ignored when tokenizing
        sc (Spark Context):         Spark Context Environment
        mdb_results_collection (pymongo collection): a mongoDB collection pointer to the recResult collection
                                    similarity score.
    Returns:
        similaritiesRDD (RDD):      RDD of cosine similarities between the submitted documents and the query documents
        recommendations (list):     an accending sorted list of recommendations for the submitted documents
    """

    #run checking system to see if this ticket already has a usable result stored in the recResult collection
    #if this checking system returns False, then run the ticket through the engine
    lastPreProcessTime = recConfig_handle.lastProcessedDate

    usableResultCheck = submissionChecks.checkResultTiming(inputSub,mdb_results_collection,lastPreProcessTime)
    if usableResultCheck == True:
        print "\n--------------------------------------------------"
        print "A usable result for {'inputId':ObjectId(\'%s\')} already exists in the %s collection:\n" % (inputSub,mdb_results_collection.name)
        print mdb_results_collection.find_one({'inputId':ObjectId(inputSub)})
        #Don't run the engine
        return

    print "\nQueuing ObjectId(\'%s\') for recommendation..." % (inputSub)
    #initialize the recResult document
    if submission_type == 'query':
        current_time = datetime.datetime.utcnow()
        mdb_results_collection.update_one({'inputId':ObjectId(inputSub)},{"$set":{'status':0,'groupingId':groupingId,'startTime':current_time}},upsert=True)

    #DEFAULT FOR TESTING PURPOSES
    #mdb_collectionType="incidents"

    print "\nRunning the recommendation engine on ObjectId(\'%s\')..." % (inputSub)
    if submission_type == 'query':
        mdb_results_collection.update_one({'inputId':ObjectId(inputSub)},{"$set":{'status':1}},upsert=True)

    #get the format of the mongodb query
    query_format = grabQueryFormat(mdb_collectionType=mdb_collectionType)

    #grab the RDD of the input ObjectId or list of input ObjectIds
    subRDD,insertedIds = setupInputSubRDD(inputSub,mdb_collection,query_format,sc,submission_type)

    #run the recommendation function with the configured variables
    if submission_type == 'query':
        mdb_results_collection.update_one({'inputId':ObjectId(inputSub)},{"$set":{'status':2}},upsert=True)

    #grab the querry RDD
    queryRDD = recommend(subRDD,mdb_collection,rlvcStats_collection,groupingId,query_format,sw_set,sc)
    #give the input submission RDD and the query RDD to the similarity engine
    similarities_recRDD,recommendations = getSimilarityScore(subRDD,queryRDD,sw_set,sc)
    print "Done!"

    #return similarities_recRDD,recommendations

    #---------------------------------------------------------------------------------
    #now run the engine for summary and details fields for the top 20% of the results:
    #---------------------------------------------------------------------------------
    firstPassTop20Percent = int(len(recommendations) * 0.2)

    #get a list of IDs
    firstPassList = topNRecommendations(recommendations,firstPassTop20Percent,inputSub)
    firstPassIds = [d['outputId'][1] for d in firstPassList]

    #setup the input submission and query RDDs for the second pass of the similarity engine
    secondPass_query_format = grabQueryFormat(mdb_collectionType=mdb_collectionType,firstPass=False)
    secondPass_subRDD,secondPass_insertedIds = setupInputSubRDD(inputSub,mdb_collection,secondPass_query_format,sc,submission_type)
    secondPass_queryRDD = setup_query(firstPassIds,mdb_collection,secondPass_query_format,sc)

    #run the second pass of the similarity engine
    secondPass_similarities_recRDD,secondPass_recommendations = getSimilarityScore(secondPass_subRDD,secondPass_queryRDD,sw_set,sc)

    #join the first and second pass scores
    joint_similarities = similarities_recRDD.join(secondPass_similarities_recRDD)
    #apply the first pass's score as a weighting to the second pass (0.5 SHOULD BE TAKEN FROM recConfig_handle)
    joint_similarities_weighted = joint_similarities.map(lambda rec: weighting.applyWeights(rec,0.5)).cache()
    #grab a sorted list of the results
    twoPass_recommendations = sorted(joint_similarities_weighted.collect(), key=lambda similarity: similarity[1])

    if weighted == True:

        print "\nFetching the rlvcScore values from the recommended documents for Relevancy weighting..."

        recIdList = grabIdList(twoPass_recommendations)
        #grab from mongoDB
        recommendedDocs = mdb_collection.find({'_id':{'$in':recIdList}},{'rlvcScore':1})

        #get rlvcScore RDD and recScore RDD
        rlvcList = weighting.getRelevancy(recommendedDocs)
        rlvcListRDD = sc.parallelize(rlvcList)

        recList = weighting.parseRecommendation(twoPass_recommendations)
        recListRDD = sc.parallelize(recList)

        #combine the lists into one
        grouped_rec_rlvc = recListRDD.join(rlvcListRDD)
        print "Done!"

        #apply relevancy weighting
        overallRlvcWeight = recConfig_handle.overallRlvcWeight
        print "\nApplying Relevancy weighting with an overall weighting of: %f..." % (overallRlvcWeight)
        final_score = grouped_rec_rlvc.map(lambda rec: weighting.applyRlvc(rec,overallRlvcWeight)).cache()
        final_recommendations = sorted(final_score.collect(), key=lambda similarity: similarity[1])

        #get a list of recommendations and their scores
        n = recConfig_handle.recTotalOutput
        resultsList = topNRecommendations(final_recommendations,n,inputSub)
        print "Done!"

        print "\nStoring the top %i results to the %s collection..." % (n,mdb_results_collection.name)
        #update the recResult document with the recommendations
        if submission_type == 'query':
            mdb_results_collection.update_one({'inputId':ObjectId(inputSub)},{"$set":{'output':resultsList}},upsert=True)
            #update the endTime and status fields of recResult
            current_time = datetime.datetime.utcnow()
            mdb_results_collection.update_one({'inputId':ObjectId(inputSub)},{"$set":{'status':3,'endTime':current_time,'result':1}},upsert=True)
        print "Done!"

        #show results
        print "\n--------------------------------------------------"
        print "Results of the recommendation engine for {'inputId':ObjectId(\'%s\')} have been successfully stored in the %s collection:\n" % (inputSub,mdb_results_collection.name)
        print mdb_results_collection.find_one({'inputId':ObjectId(inputSub)})

        #return similarities_recRDD,recommendations,secondPass_similarities_recRDD,secondPass_recommendations,twoPass_recommendations,final_score,final_recommendations
        #return recommendations,secondPass_recommendations,twoPass_recommendations,final_recommendations

        #return the results
        #return similarities_recRDD,recommendations,insertedIds

#    #if using weights
#    if weighted == True:
#        """
#        #--------------------------------#
#        #  Rework Using Relevancy Score  #
#        #--------------------------------#
#        """
#        #get the weighting MongoDB query format
#        #weight_query_format = weighting.grabWeightingQueryFormat(mdb_collectionType)
#        weight_query_format = {"summary":1, "details":1}
#        #create the input weighting RDD
#        weight_subRDD = setup_query(insertedIds,mdb_collection,weight_query_format,sc)
#        #generate the weights
#        similarities_weightRDD,weights = weighting.getWeightScore(weight_subRDD,mdb_collection,weight_query_format,sc)
#
#        #group the recommendations and the weights together
#        grouped_rec_weight = similarities_recRDD.join(similarities_weightRDD)
#
#        #apply the weights to the recommendations and get the final score
#        final_score = grouped_rec_weight.map(lambda rec: weighting.applyWeights(rec)).cache()
#        final_recommendations = sorted(final_score.collect(), key=lambda similarity: similarity[1])
#
#        similarities_recRDD = final_score
#        recommendations = final_recommendations
#
#    else:
#        print recommendations[-20:][:10]
#        if submission_type != 'query':
#            mdb_collection.delete_many({"_id":{"$in":insertedIds}})
#        print mdb_collection.count()
#
#    #get a list of recommendations and their scores
#    resultsList = topNRecommendations(recommendations,n)
#
#    #update the recResult document with the recommendations
#    if submission_type == 'query':
#        mdb_results_collection.update_one({'inputId':ObjectId(inputSub)},{"$set":{'output':resultsList}},upsert=True)
#        #update the endTime and status fields of recResult
#        current_time = datetime.datetime.utcnow()
#        mdb_results_collection.update_one({'inputId':ObjectId(inputSub)},{"$set":{'status':3,'endTime':current_time,'result':1}},upsert=True)
#
#    #show results
#    print mdb_results_collection.find_one({'inputId':ObjectId(inputSub)})
#
#    #return the results
#    #return similarities_recRDD,recommendations,insertedIds

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
    #mdb_uri = "mongodb://atlasuser:atlasuserpw#1@dev.puzzlelogic.com:27017/atlas"
    mdb_uri = "mongodb://plasmuser:plasmpass@10.10.10.243:27017/plasm"
    if (len(sys.argv)>3): mdb_uri=str(sys.argv[3])

    #<groupingId>
    #"538f9e119e16abd715913ab5"
    groupingId = "5708279864a89c6f1b5bd28f"
    if (len(sys.argv)>4): groupingId=str(sys.argv[4])

    #<mdb_message_collection_name>
    #"message"
    mdb_collection_name="message"
    if (len(sys.argv)>5): mdb_collection_name=str(sys.argv[5])

    #<mdb_results_collection>
    mdb_results_collection_name="recResult"
    if (len(sys.argv)>6): mdb_results_collection_name=str(sys.argv[6])

    #<rlvcStats_collection>
    rlvcStats_collection_name="rlvcStats"
    if (len(sys.argv)>7): rlvcStats_collection_name=str(sys.argv[7])

    #create the mongoBall and get the collection connector
    mdb = mongoURIBall(mdb_uri)
    mdb_collection = mdb.collection(mdb_collection_name)
    print "\nConnecting to the %s collection" % (mdb_collection_name)

    #connect to the recResult collection
    mdb_results_collection = mdb.collection(mdb_results_collection_name)
    print "\nConnecting to the %s collection" % (mdb_results_collection_name)

    #connect to the rlvcStats collection
    rlvcStats_collection = mdb.collection(rlvcStats_collection_name)
    print "\nConnecting to the %s collection" % (rlvcStats_collection_name)

    #get the MongoDB collection ticket type
    mdb_collectionType="incidents"
    if (len(sys.argv)>8): mdb_collectionType=str(sys.argv[8])


    #get whether the results should be weighted or not
    weighted=True
    if (len(sys.argv)>9): weighted=str(sys.argv[9]).lower() in ("yes","true","t","1")

    #get the number of recommendations the user wants returned
    recConfig_collection = "recConfig"
    if (len(sys.argv)>10): recConfig_collection=str(sys.argv[10])
    recConfig_uri = mdb_uri
    if (len(sys.argv)>11): recConfig_uri=str(sys.argv[11])

    #get the recConfig collection connector
    recConfig_handle = recConfigURI(mongoURI=recConfig_uri,collection=recConfig_collection,groupingId=groupingId)

    #with all of the required variables gathered, run the engine
    run_engine(submission_type,inputSub,groupingId,mdb_collection,mdb_collectionType,sw_set,sc,mdb_results_collection,rlvcStats_collection,recConfig_handle,weighted=weighted)

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
    sw = stopwords()
    sw_set = sw.stopwords_set

    #execute main functionality
    main(sc,sw,sw_set)
