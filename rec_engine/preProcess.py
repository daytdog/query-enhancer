#!/usr/bin/python
#preProcess.py

#packages from rec_engine
from rec_engine import textAnalyzer as ta
from rec_engine import weighting
from rec_engine import algorithms
from rec_engine.mongoBall import mongoURIBall,recConfigURI
from rec_engine.stopwords import stopwords

#packages outside of rec_engine
import sys,datetime
import numpy as np

"""
preProcess.py is the core application that is submitted to spark-submit
for the relevancy engine. This application is called in the following way:

    ~$ preProcess.py groupingId mongoDB_uri <mongoDB_collection> <rlvcStats_collection> <recConfig_uri> <recConfig_collection>

    Args:
        groupingId (str):           The groupingId that the input document belongs
                                    to in the Mongo Database.

        mongoDB_uri (str):          URI of the Mongo database that contains the
                                    documents to be recommended against.

    (Optional):
        mongoDB_collection (str):   Name of the MongoDB collection that contains
                                    the documents that will be recommended.
                                    (default: "message")

        rlvcStats_collection (str): Name of the MongoDB collection that contains
                                    the statistics of the rlvcScores of the given
                                    groupingId
                                    (default: "rlvcStats")

        recConfig_collection (str): Name of the MongoDB collection that contains
                                    user-configurable variables for use in both
                                    the recommendation engine and the pre-
                                    processing engine
                                    (default: "recConfig")

        recConfig_uri (str):        URI of the Mongo Database that contains the
                                    user-configurable variables documents
                                    (default: same as mongoDB_uri)
"""

APP_NAME = "Relevancy Engine Pre-Process"

def usage():
    print __doc__
    sys.exit()


def grabWeightingQuery(groupingId,mdb_collection,mdb_collectionType="incidents",withRlvcScore=False):
    #grab the query format (default to "incidents" type messages)
    weight_query_format = weighting.grabWeightingQueryFormat(mdb_collectionType)

    #get a handle on this query from the mongoDB collection
    weight_fields_query = mdb_collection.find({'groupingId':groupingId,'preProcessTag':{"$exists":False},'rlvcScore':{"$exists":withRlvcScore}},weight_query_format)


    return weight_fields_query

def tokenizeFieldCount(field_name,doc,sw_set):
    return len(ta.tokenize(doc[field_name],sw_set))

def generateFieldValuesForSingleDoc(doc,sw_set):
    """
    returns a dictionary of field values for the recConfig parameters
    """
    #initialize the dictionary
    configResults = {'linked_id':doc['_id'],'groupingId':doc['groupingId']}

    #get summary and details tokenized value counts
    field_names = ['summary','details']
    for field in field_names:
        if doc.get(field) != None:
            configResults[field] = tokenizeFieldCount(field,doc,sw_set)

    if doc.get('workNotes') != None:
        #get count of number of workNotes
        workNotesList = doc['workNotes']
        numWorkNotes = len(workNotesList)

        #get the median and standard deviation of the workNotes if there are 10 or more workNotes
        if numWorkNotes >= 10:
            data = []
            for note in workNotesList:
                if note.get(field_names[0]) != None and note.get(field_names[1]) != None:
                    data.append((tokenizeFieldCount(field_names[0],note,sw_set),tokenizeFieldCount(field_names[1],note,sw_set)))
            dataArray = np.asarray(data)
            sumMed = np.median(dataArray[:,0]); detMed = np.median(dataArray[:,1])
            sumStd = np.std(dataArray[:,0]); detStd = np.std(dataArray[:,1])

            popList = []
            #filter out workNotes with low summary and details tokens
            for i in np.arange(len(dataArray)):
                if dataArray[i][0] <= 3:
                    popList.append(i)
                elif dataArray[i][1] <= 10:
                    popList.append(i)
            popList.reverse()

            for i in popList:
                workNotesList.pop(i)

            numWorkNotes = len(workNotesList)
        configResults['workNotes'] = numWorkNotes

        #if the number of workNotes is not 0, get the largest summary and details counts
        if numWorkNotes > 0:
            for field in field_names:
                data = []
                for note in workNotesList:
                    if note.get(field) != None:
                        data.append(tokenizeFieldCount(field,note,sw_set))
                fieldName = 'workNotes.%s' % (field)
                if len(data) != 0:
                    configResults[fieldName] = max(data)

    #get sortedPriority value
    if doc.get('sortedPriority') != None:
        configResults['sortedPriority'] = int(doc['sortedPriority'])

    #get createdDate and closedDate time difference (in seconds)
    #REMOVE THIS WEIGHTING COMPONENT
    if (doc.get('createdDate') != None) and (doc.get('closedDate') != None):
        createdToCloseTimeSeconds = (doc['closedDate'] - doc['createdDate']).total_seconds()
        configResults['createdToCloseTime'] = createdToCloseTimeSeconds

    #get reportedDate and resolutionDate time difference (in seconds)
    if (doc.get('reportedDate') != None) and (doc.get('resolutionDate') != None):
        reportedToResolvedTimeSeconds = (doc['resolutionDate'] - doc['reportedDate']).total_seconds()
        configResults['reportedToResolvedTime'] = reportedToResolvedTimeSeconds

    #return the created dictionary
    return configResults

def getWeightsList(configResults,recConfig_handle):
    """ returns a list of weights based off the configResults dictionary
        compared with the recConfig parameters of the given groupingId
    """

    #verbosity of the output
    verbose = False

    #Fields to add to Carolyn's recConfig document:
    #missingFieldPenalty    : 0.7
    #missingFieldThreshold  : 2
    #upperNumTokensBound    : 150

    #variables to be set now, will be configurable in the future
    missingFieldPenalty = 0.7
    upperNumTokensBound = 150
    #keep track of the number of fields that are going missing
    missingFieldCount = 0
    #after how many missing fields will the penalty be applied
    missingFieldThreshold = 2
    penalty = 1.0

    #initialize the list of weights with no initial weight (1.0)
    weightsList = [1.0]

    if verbose == True:
        print ""
    #get the summary weight
    if configResults.get('summary') != None:
        if configResults['summary'] >= recConfig_handle.rlvcNumSummaryWords:
            summaryTokens_weight = algorithms.logisticsGrowth2(configResults['summary'],upperNumTokensBound=upperNumTokensBound,minNumTokens=recConfig_handle.rlvcNumSummaryWords,upperBoundWeight=recConfig_handle.rlvcNumSummaryWords_wt)
            weightsList.append(summaryTokens_weight)
            if verbose == True:
                print "with %s summary tokens, weight Score: %s" % (configResults['summary'],summaryTokens_weight)
            #weightsList.append(recConfig_handle.rlvcNumSummaryWords_wt)
    else:
        missingFieldCount += 1

    #get the details weight
    if configResults.get('details') != None:
        if configResults['details'] >= recConfig_handle.rlvcNumDetailsWords:
            detailsTokens_weight = algorithms.logisticsGrowth2(configResults['details'],upperNumTokensBound=upperNumTokensBound,minNumTokens=recConfig_handle.rlvcNumDetailsWords,upperBoundWeight=recConfig_handle.rlvcNumDetailsWords_wt)
            weightsList.append(detailsTokens_weight)
            if verbose == True:
                print "with %s details tokens, weight Score: %s" % (configResults['details'],detailsTokens_weight)
            #weightsList.append(recConfig_handle.rlvcNumDetailsWords_wt)
    else:
        missingFieldCount += 1

    #get the number of workNotes weight
    if configResults.get('workNotes') != None:
        if configResults['workNotes'] >= recConfig_handle.rlvcNumWorkNotes:
            numWorkNotes_weight = algorithms.logisticsGrowth2(configResults['workNotes'],upperNumTokensBound=20,minNumTokens=recConfig_handle.rlvcNumWorkNotes,upperBoundWeight=recConfig_handle.rlvcNumWorkNotes_wt)
            weightsList.append(numWorkNotes_weight)
            if verbose == True:
                print "Number of WorkNotes weight Score: %s" % (numWorkNotes_weight)
            #weightsList.append(recConfig_handle.rlvcNumWorkNotes_wt)

        elif configResults['workNotes'] == 0:
            #if the workNotes field is just empty, it's the same as having a missing field
            missingFieldCount += 1
    else:
        missingFieldCount += 1

    #get the workNotes.summary weight
    if configResults.get('workNotes.summary') != None:
        if configResults['workNotes.summary'] >= recConfig_handle.rlvcNumWorkNoteSummaryWords:
            workNotesSummaryTokens_weight = algorithms.logisticsGrowth2(configResults['workNotes.summary'],upperNumTokensBound=upperNumTokensBound,minNumTokens=recConfig_handle.rlvcNumWorkNoteSummaryWords,upperBoundWeight=recConfig_handle.rlvcNumWorkNoteSummaryWords_wt)
            weightsList.append(workNotesSummaryTokens_weight)
            if verbose == True:
                print "with %s workNotes.summary tokens, weight Score: %s" % (configResults['workNotes.summary'],workNotesSummaryTokens_weight)
            #weightsList.append(recConfig_handle.rlvcNumWorkNoteSummaryWords_wt)
    else:
        missingFieldCount += 1

    #get the workNotes.details weight
    if configResults.get('workNotes.details') != None:
        if configResults['workNotes.details'] >= recConfig_handle.rlvcNumWorkNoteDetailsWords:
            workNotesDetailsTokens_weight = algorithms.logisticsGrowth2(configResults['workNotes.details'],upperNumTokensBound=upperNumTokensBound,minNumTokens=recConfig_handle.rlvcNumWorkNoteDetailsWords,upperBoundWeight=recConfig_handle.rlvcNumWorkNoteDetailsWords_wt)
            weightsList.append(workNotesDetailsTokens_weight)
            if verbose == True:
                print "with %s workNotes.details tokens, weight Score: %s" % (configResults['workNotes.details'],workNotesDetailsTokens_weight)
            #weightsList.append(recConfig_handle.rlvcNumWorkNoteDetailsWords_wt)
    else:
        missingFieldCount += 1

    #-------------------------------------------------------------------------
    #IGNORE THE WEIGHT OF THE DIFFERENCE BETWEEN THE CREATED AND CLOSED DATES!
    #-------------------------------------------------------------------------
    #get createdToCloseTime weight
    #if configResults.get('createdToCloseTime') != None:
    #    mu = algorithms.muCalculator(lowerBound=recConfig_handle.rlvcTimeRangeLowerBoundCreatedDate,upperBound=recConfig_handle.rlvcTimeRangeUpperBoundCreatedDate)
    #    sigma = algorithms.sigmaCalculator(recConfig_handle.rlvcTimeRangeLowerBoundCreatedDate,recConfig_handle.rlvcTimeRangeLowerBoundCreatedDate_wt,recConfig_handle.rlvcTimeRangeUpperBoundCreatedDate_wt,mu)
    #    gaussian_result = algorithms.gaussian(configResults['createdToCloseTime'],mu,sigma,recConfig_handle.rlvcTimeRangeUpperBoundCreatedDate_wt)
    #    weightsList.append(gaussian_result)

    #get reportedToResolvedTime weight
    if configResults.get('reportedToResolvedTime') != None:
        mu = algorithms.muCalculator(lowerBound=recConfig_handle.rlvcTimeRangeLowerBound,upperBound=recConfig_handle.rlvcTimeRangeUpperBound)
        sigma = algorithms.sigmaCalculator(recConfig_handle.rlvcTimeRangeLowerBound,recConfig_handle.rlvcTimeRangeLowerBound_wt,recConfig_handle.rlvcTimeRangeUpperBound_wt,mu)
        gaussian_result = algorithms.gaussian(configResults['reportedToResolvedTime'],mu,sigma,recConfig_handle.rlvcTimeRangeUpperBound_wt)
        weightsList.append(gaussian_result)
        if verbose == True:
            print "with %s second between reported and resolved dates, weight score: %s" % (configResults['reportedToResolvedTime'],gaussian_result)
    else:
        missingFieldCount += 1


    #deal with the missing fields counter
    if missingFieldCount > missingFieldThreshold:
        penalty = missingFieldPenalty**(missingFieldCount - missingFieldThreshold)
        #weightsList.append(penalty)
        if verbose == True:
            print "%s fields were missing from this document, causing a penalty of %s" % (missingFieldCount,penalty)

    #possibly pass the penalty and apply it outside of here, possibly apply it post-inverseWeighting
    #return the list of weights
    return weightsList,penalty


def calculateRlvcWeight(doc,sw_set,recConfig_handle):
    """ calculates the relevancy weighting for a given document
    """

    #get configResults dictionary
    configResults = generateFieldValuesForSingleDoc(doc,sw_set)

    #get the list of weights
    weightsList,penalty = getWeightsList(configResults,recConfig_handle)

    #calculate the total relevancy score for the document
    rlvcScore = algorithms.inverseWeighting(weightsList)*penalty

    #apply the sortedPriorty weight
    #get sortedPriority weight
    if configResults.get('sortedPriority') != None:
        #flip the order of sortedPriority so that a higher number indicates a more critical priority
        sortedPriorityFlipped = recConfig_handle.rlvcNumPriorityLevels - configResults['sortedPriority'] - 1
        sortedPriority_weight = algorithms.cosineGrowth(sortedPriorityFlipped,numPriorityLevels=recConfig_handle.rlvcNumPriorityLevels,maxPriorityWeight=recConfig_handle.rlvcNumPriorityLevels_wt)
        #weightsList.append(algorithms.priorityLevelWeighting(configResults['sortedPriority'],priorityLevelsMaxWeight=recConfig_handle.rlvcNumPriorityLevels_wt,numPriorityLevels=recConfig_handle.rlvcNumPriorityLevels))
        rlvcScore *= sortedPriority_weight
    else:
        rlvcScore *= 0.7

    return rlvcScore

def rlvcStatistics(topNPercent,groupingId,mdb_collection,rlvcStats_collection,recEngineInputLimit=100000):
    """ Creates a MongoDB table with the relevancy score statistics of the current run
    Args:
        topNPercent (float):        Decimal value of the top N percent that the user wants
                                        to use in the recommendation engine

        recEngineInputLimit (int):  Limit of the number of documents that can be input into
                                        the recommendation engine (default: 100,000 docs)
    """
    #grab the rlvcScores of the current run and place them into a list
    rlvcDocuments = mdb_collection.find({'groupingId':groupingId,'preProcessTag':{"$exists":False}},{'rlvcScore':1})
    rlvcList = []
    for rec in rlvcDocuments:
        rlvcList.append(rec['rlvcScore'])
    rlvcListSorted = np.sort(rlvcList)
    rlvcCount = len(rlvcListSorted)

    #get the length of the list of the top N percent of rlvcScores
    percentFilterCount = int(rlvcCount * topNPercent)
    #if this length is higher than the limit, then use the limit for the cutoff index
    if percentFilterCount > recEngineInputLimit:
        filterCutoffIndex = recEngineInputLimit + 1
    else:
        filterCutoffIndex = percentFilterCount + 1
    #get the cutoff value for the top N percent of rlvcScore within the limit
    filterCutoffRlvcScore = rlvcListSorted[-filterCutoffIndex]

    #get the average, median, and std values of the overall and filtered lists
    rlvcScoreArray = np.asarray(rlvcListSorted)
    topNPercentScores = rlvcScoreArray[-filterCutoffIndex:]

    #create a dictionary to add to the statistics collection document
    inputDictionary = {mdb_collection.name:{'cutoffRlvcScore':filterCutoffRlvcScore,'filteredDocsCount':filterCutoffIndex-1}}

    #upsert the document to the statistics collection
    rlvcStats_collection.update_one({'groupingId':groupingId},{"$set":inputDictionary},upsert=True)

def preProcessing_check(doc,previousRunTimestamp):
    """ rlvcScore calculations will not be done for a given doc if it meets any of the following criteria:
        - if the doc already has a rlvcScore and its lastModified timestamp is before the previous
          preProcessing run's timestamp (i.e. something was changed in the document since the last
          preProcessing run)
        - doc is already tagged for preProcessing by another preProcess.py process
    """
    pass



def run_preProcessing(groupingId,mdb_collection,mdb,rlvcStats_collection,recConfig_collection,sw_set,recConfig_handle):
    """ Run the preProcessing/relevancy engine
    """
    engine_start_time = datetime.datetime.utcnow()

    #grab the query format (default to "incidents" type messages)
    weight_query_format = weighting.grabWeightingQueryFormat("incidents")

    #create the queries for the new and old documents
    newDocQuery = {'groupingId':groupingId,'preProcessTag':{"$exists":False},'rlvcScore':{"$exists":False}}
    oldDocQuery = {'groupingId':groupingId,'preProcessTag':{"$exists":False},'rlvcScore':{"$exists":True},'lastModified':{"$gt":recConfig_handle.lastProcessedDate}}
    #oldDocQuery = {'groupingId':groupingId,'preProcessTag':{"$exists":False},'rlvcScore':{"$exists":True}}
    #oldDocQuery = {'groupingId':groupingId,'rlvcScore':{"$exists":True}}

    #for when the 'lastModified' field is added to the recConfig collection
    """
    if (recConfig_handle.lastProcessedDate - recConfig_handle.lastModified).total_seconds() < 0:
        oldDocQuery = {'groupingId':groupingId,'preProcessTag':{"$exists":False},'rlvcScore':{"$exists":True}}
    else:
        oldDocQuery = {'groupingId':groupingId,'preProcessTag':{"$exists":False},'rlvcScore':{"$exists":True},'lastModified':{"$lt":recConfig_handle.lastProcessedDate}}
    """

    print 'Tagging Documents for preProcessing Using Timestamp: %s...' % (engine_start_time)
    #tag the documents in the queries for preProcessing
    mdb_collection.update_many(newDocQuery,{"$set":{'preProcessTag':engine_start_time}})
    mdb_collection.update_many(oldDocQuery,{"$set":{'preProcessTag':engine_start_time}})
    print 'Done!\n'

    #update the queries to include just the preProcessTag timestamp for this process
    newDocQuery['preProcessTag'] = {"$eq":engine_start_time}
    oldDocQuery['preProcessTag'] = {"$eq":engine_start_time}

    #grab the new documents in the given groupingId from mongoDB
    newDocuments = mdb_collection.find(newDocQuery,weight_query_format)
    #grab the query for documents that need their rlvcScores updated
    oldDocuments = mdb_collection.find(oldDocQuery,weight_query_format)

    #run any new documents without 'rlvcScore' fields through the preProcessing engine
    docCount = 0
    #calculate their relevancy score
    print 'Running Engine for New Documents Without rlvcScores...'
    for doc in newDocuments:
        docId = doc['_id']
        rlvcScore = calculateRlvcWeight(doc,sw_set,recConfig_handle)
        print "inserted document #%i" % (docCount)
        docCount +=1
        mdb_collection.update_one({'_id':docId},{"$set":{'rlvcScore':rlvcScore}},upsert=True)
    print 'Done!\n'

    #run the engine for any documents with rlvcScores that need to be updated
    print 'Running Engine for Old Documents That Need Updating...'
    for doc in oldDocuments:
        docId = doc['_id']
        rlvcScore = calculateRlvcWeight(doc,sw_set,recConfig_handle)
        print "inserted document #%i" % (docCount)
        docCount +=1
        mdb_collection.update_one({'_id':docId},{"$set":{'rlvcScore':rlvcScore}},upsert=True)
    print 'Done!\n'

    print 'Removing preProcessing Tags from Documents...'
    mdb_collection.update_many(newDocQuery,{"$unset":{'preProcessTag':1}})
    mdb_collection.update_many(oldDocQuery,{"$unset":{'preProcessTag':1}})
    print 'Done!\n'

    #update the rlvcStats collection
    print 'Updating the rlvcStats Collection...'
    topNPercent = recConfig_handle.filterTopPercent
    recEngineInputLimit = recConfig_handle.recEngineInputLimit
    rlvcStatistics(topNPercent,groupingId,mdb_collection,rlvcStats_collection,recEngineInputLimit)
    print 'Done!\n'

    #update the lastProcessedDate timestamp
    print 'Updating the lastProcessedDate Timestamp in the recConfig Collection...'
    engine_end_time = datetime.datetime.utcnow()
    recConfig_collection.update_one({'groupingId':groupingId},{"$set":{'lastProcessedDate':engine_end_time}},upsert=True)
    print 'Done!\n'
    print 'preProcessing Has Been Completed!'

def demo_run_preProcessing(groupingId,mdb_collection,mdb,recConfig_collection,sw_set,recConfig_handle):
        documents = grabWeightingQueryNewDocs(groupingId,mdb_collection,mdb_collectionType="incidents")
        for doc in documents:
            rlvcScore = calculateRlvcWeight(doc,sw_set,recConfig_handle)
            docId = doc['_id']
            print docId, rlvcScore

def main(sw,sw_set):
    #if at least 1 argument isn't given, display usage
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
    run_preProcessing(groupingId,mdb_collection,mdb,rlvcStats_collection,recConfig_collection,sw_set,recConfig_handle)

    #objectId of an interesting ticket, will be useful for programming the components:
    #{u'_id': ObjectId('5720172e64a8f86db81c2a25')}


if __name__ == "__main__":
    #set up the stopwords list
    sw = stopwords()
    sw_set = sw.stopwords_set

    #execute main functionality
    main(sw,sw_set)
