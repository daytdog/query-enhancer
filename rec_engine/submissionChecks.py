#!/usr/bin/python
#submissionChecks.py

#packages outside of rec_engine
from bson.objectid import ObjectId


def checkResultExists(inputId,mdb_results_collection):

    #check if the results document exists in the recResult collection
    if mdb_results_collection.find({'inputId':ObjectId(inputId)}).count() > 0:
        print "\nA document for {'inputId':ObjectId(\'%s\')} has been found in the %s collection..." % (inputId,mdb_results_collection.name)
        return True
    else:
        print "\nA document for {'inputId':ObjectId(\'%s\')} was not found in the %s collection" % (inputId,mdb_results_collection.name)
        return False

def checkResultStatus(inputId,mdb_results_collection):

    resultExists = checkResultExists(inputId,mdb_results_collection)

    #if the results document exists, check the status of the document
    if resultExists == True:
        resultStatus = mdb_results_collection.find_one({'inputId':ObjectId(inputId)},{'status':1})['status']

        if resultStatus != 3:
            print "\nThe status of the previous result showed as \"incomplete\"."
            return False
        else:
            return True

    else:
        return False

def checkResultTiming(inputId,mdb_results_collection,lastPreProcessTime):

    resultStatusCheck = checkResultStatus(inputId,mdb_results_collection)

    #if the results document exists and the status is marked as finished, check that the
    #last result returned was after the last preProcess run
    if resultStatusCheck == True:
        print "Checking to make sure the previous result was produced after the most recent pre-processing run..."

        resultStartTime = mdb_results_collection.find_one({'inputId':ObjectId(inputId)},{'startTime':1})['startTime']

        timeDifference = (resultStartTime - lastPreProcessTime).total_seconds()

        if timeDifference > 0:
            return True
        else:
            print "\nThe previous result found in the %s collection was produced before the most recent pre-processing run." % (mdb_results_collection.name)
            return False

    else:
        return False

