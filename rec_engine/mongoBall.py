#!/usr/bin/python

"""
mongoBall.py
A python class that stores the information needed to connect to a
Mongo database

Attributes:
    database - name of the Mongo Database (default: 'test')
    client   - name of the Mongo host client (default: 'mongodb://localhost')
    port     - port number of the Mongo Database (default: 27017)
"""

import pymongo

class mongoBall(object):

    def __init__(self,database='test',client='mongodb://localhost',port=27017,passProtected=False,userName='admin',userPass='password'):
        """ Initializes the MongoDB setup object
        Args:
            database (str or unicode): name of the Mongo database
            client (str or unicode):   name of the Mongo database client
            port (int):                port number
        """
        self.database = database
        self.client = client
        self.port = port
        self.passProtected = passProtected
        self.userName = userName
        self.userPass = userPass

    def getDatabase(self):
        """ Return the name of the Mongo Database
        """
        return self.database

    def getClient(self):
        """ Return the name of the Mongo client host
        """
        return self.client

    def getPort(self):
        """ Return the port number
        """
        return self.port

    def stat(self):
        print "Database: ",self.database
        print "Client:   ",self.client
        print "Port:     ",self.port

    def connection(self):
        """ Establish a connection to the database
        """
        return pymongo.MongoClient(self.client,self.port)

    def db(self):
        """ Get a handle to the database
        """
        mdb = getattr(self.connection(),self.database)
        if self.passProtected == True:
            mdb.authenticate(self.userName,self.userPass)
        return mdb

    def collection(self,collection):
        """ Access collection 'collection'
        """
        return getattr(self.db(),collection)

class recConfig(mongoBall):

    def __init__(self,database='plasm',client='10.10.10.243',port=27017,passProtected=True,userName='plasmuser',userPass='plasmpass',collection='recConfig',groupingId='5708279864a89c6f1b5bd28f'):
        mongoBall.__init__(self,database,client,port,passProtected,userName,userPass)

        #connect to the database and grab the config document
        self.collection = self.collection(collection)
        self.groupingId = groupingId
        self.configDict = self.collection.find_one({'groupingId':self.groupingId},{'_id':0})

        #now that we have the config document, save the field values
        self.lastProcessedDate = self.configDict.get('lastProcessedDate')
        self.recTTL = self.configDict.get('recTTL')
        self.recTotalOutput = self.configDict.get('recTotalOutput')
        self.rlvcNumDetailsWords = self.configDict.get('rlvcNumDetailsWords')
        self.rlvcNumPriorityLevels = self.configDict.get('rlvcNumPriorityLevels')
        self.rlvcNumSummaryWords= self.configDict.get('rlvcNumSummaryWords')
        self.rlvcNumWorkNoteDetailsWords = self.configDict.get('rlvcNumWorkNoteDetailsWords')
        self.rlvcNumWorkNoteSummaryWords = self.configDict.get('rlvcNumWorkNoteSummaryWords')
        self.rlvcNumWorkNotes = self.configDict.get('rlvcNumWorkNotes')
        self.rlvcTimeRangeLowerBound = self.configDict.get('rlvcTimeRangeLowerBound')
        self.rlvcTimeRangeLowerBoundCreatedDate = self.configDict.get('rlvcTimeRangeLowerBoundCreatedDate')
        self.rlvcTimeRangeUpperBound = self.configDict.get('rlvcTimeRangeUpperBound')
        self.rlvcTimeRangeUpperBoundCreatedDate = self.configDict.get('rlvcTimeRangeUpperBoundCreatedDate')

class recConfig_weights(mongoBall):

    def __init__(self):
        #define the weighting values if the parameters are met
        self.rlvcNumDetailsWords_wt = 1.3
        self.rlvcNumPriorityLevels_wt = 1.5
        self.rlvcNumSummaryWords_wt = 1.3
        self.rlvcNumWorkNoteDetailsWords_wt = 1.4
        self.rlvcNumWorkNoteSummaryWords_wt = 1.4
        self.rlvcNumWorkNotes_wt = 1.5
        self.rlvcTimeRangeLowerBound_wt = 1.1
        self.rlvcTimeRangeLowerBoundCreatedDate_wt = 1.1
        self.rlvcTimeRangeUpperBound_wt = 1.4
        self.rlvcTimeRangeUpperBoundCreatedDate_wt = 1.4

class mongoURIBall(mongoBall):

    def __init__(self,mongoURI="mongodb://atlasuser:atlasuserpw#1@dev.puzzlelogic.com:27017/atlas"):
        self.mongoURI = mongoURI

    def connection(self):
        return pymongo.MongoClient(self.mongoURI)

    def db(self):
        return self.connection().get_default_database()

class recConfigURI(mongoURIBall):

    def __init__(self,mongoURI="mongodb://plasmuser:plasmpass@10.10.10.243:27017/plasm",collection='recConfig',groupingId='5708279864a89c6f1b5bd28f'):

        mongoURIBall.__init__(self,mongoURI)

        #connect to the database and grab the config document
        self.collection = self.collection(collection)
        self.groupingId = groupingId
        self.configDict = self.collection.find_one({'groupingId':self.groupingId},{'_id':0})

        #now that we have the config document, save the field values
        self.lastProcessedDate = self.configDict.get('lastProcessedDate')
        self.recTTL = self.configDict.get('recTTL')
        self.recTotalOutput = self.configDict.get('recTotalOutput')
        self.rlvcNumDetailsWords = self.configDict.get('rlvcNumDetailsWords')
        self.rlvcNumPriorityLevels = self.configDict.get('rlvcNumPriorityLevels')
        self.rlvcNumSummaryWords= self.configDict.get('rlvcNumSummaryWords')
        self.rlvcNumWorkNoteDetailsWords = self.configDict.get('rlvcNumWorkNoteDetailsWords')
        self.rlvcNumWorkNoteSummaryWords = self.configDict.get('rlvcNumWorkNoteSummaryWords')
        self.rlvcNumWorkNotes = self.configDict.get('rlvcNumWorkNotes')
        self.rlvcTimeRangeLowerBound = self.configDict.get('rlvcTimeRangeLowerBound')
        self.rlvcTimeRangeLowerBoundCreatedDate = self.configDict.get('rlvcTimeRangeLowerBoundCreatedDate')
        self.rlvcTimeRangeUpperBound = self.configDict.get('rlvcTimeRangeUpperBound')
        self.rlvcTimeRangeUpperBoundCreatedDate = self.configDict.get('rlvcTimeRangeUpperBoundCreatedDate')
