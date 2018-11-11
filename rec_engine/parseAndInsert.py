#!/usr/bin/python
#parseAndInsert.py

"""
This script takes a file that contains json documents on each
line and parses the file into individual json documents which
are then stored on the Mongo Database in a master collection

parseAndInsert.py infile.json <database="test"> <mclient="mongodb://localhost"> <collection="master"> <port=27017> <chunkSize=10>
"""

import sys,os,json
from mongoBall import mongoBall
import jsonTools

def usage():
    print __doc__
    sys.exit()

def paiManyMongoBall(infile,database="test",client="mongodb://localhost",collection="master",port=27017,chunkSize=10,dropped=True):
    """ Parse and insert many json documents at a time into a Mongo Database collection
    Args:
        infile (str or unicode):     name of a text file containing json documents
        database (str or unicode):   name of the Mongo Database
        client (str or unicode):     name of the Mongo host client
        port (int):                  port number of the Mongo Database
        collection (str or unicode): name of the Mongo Database collection
        chunkSize (int):             number of json documents to be inserted at a time
    """

    #setup a MongoBall
    mongoBall_new = mongoBall(database=database,client=client,port=port)

    #access the collection
    mdbCollection = mongoBall_new.collection(collection)

    #drop existing collection (FOR TESTING PURPOSES ONLY)
    if dropped == True:
        mdbCollection.drop()
    data = []

    #while there are more lines of json documents than chunkSize,
    #insert those chunks of lines into the collection
    with open(infile) as f:
        for line in f:
            if (len(data) >= chunkSize):
                print "Inserting chunk of data"
                try:
                    mdbCollection.insert_many(data)
                except Exception as e:
                    print "Unexpected error:", type(e), e
                data = []
            data.append(json.loads(line))

    #if there are less lines of json documents than chunkSize,
    #insert the rest of those lines into the collection
    print "Inserting the rest of the data"
    try:
        mdbCollection.insert_many(data)
    except Exception as e:
        print "Unexpected error:", type(e), e

def paiWithGrouping(infile,database="test",client="mongodb://localhost",collection="master",port=27017,chunkSize=10):
    """ Group, parse, and insert many json documents at a time into a Mongo Database collection
    Args:
        infile (str or unicode):     name of a text file containing json documents
        database (str or unicode):   name of the Mongo Database
        client (str or unicode):     name of the Mongo host client
        port (int):                  port number of the Mongo Database
        collection (str or unicode): fallback name of the Mongo Database collection
        chunkSize (int):             number of json documents to be inserted at a time
    """

    #grab the first json document from the file
    with open(infile, 'r') as f:
        json_doc = json.loads(f.readline())

    #determine the collection group
    try:
        collection = jsonTools.getGroup(json_doc)
        print "Using collection group \"%s\"" % collection
    except Exception as e:
        print "Error in determining the document group:", type(e), e
        print "Using fallback collection group \"%s\"" % collection

    #parse and insert the json documents into their collection group
    paiManyMongoBall(infile,database,client,collection,port,chunkSize)

def paiManyWithCollection(infile,mdb_collection):
    """ Simple parse and insert many into a given MongoDB collection and returns a list of inserted ids
    Args:
        infile (str or unicode):             name of a text file containing json documents
        mdb_collection (pymongo collection): a mongoDB collection pointer
    Returns:
        insertedIds (list):                  a list of the ids of the documents that were submitted
    """

    #open the file and insert the lines of json documents into a list
    data = []
    with open(infile) as f:
        for line in f:
            data.append(json.loads(line))

    #insert the json documents into the collection and save the ids they are assigned
    print "Inserting %i documents into the \"%s\" MongoDB collection:"% (len(data),str(mdb_collection.name))
    try:
        insertion = mdb_collection.insert_many(data)
        print "Insertion successful!"
    except Exception as e:
        print "Unexpected error:", type(e), e

    insertedIds = insertion.inserted_ids

    return insertedIds

def main():
    if(len(sys.argv)<2): usage()

    infile = sys.argv[1]
    if(os.path.exists(infile)==0):
        print "infile = %s does not exist" % infile
        usage()

    database = "test"
    if(len(sys.argv)>2): database = sys.argv[2]
    if((type(database) != str) and (type(database) != unicode)):
        print "database variable type must either be a string or unicode"
        usage()

    client = "mongodb://localhost"
    if(len(sys.argv)>3): client = sys.argv[3]
    if((type(client) != str) and (type(client) != unicode)):
        print "client variable type must either be a string or unicode"
        usage()

    collection = "master"
    if(len(sys.argv)>4): collection = sys.argv[4]
    if((type(collection) != str) and (type(collection) != unicode)):
        print "collection variable type must either be a string or unicode"
        usage()

    port = 27017
    if(len(sys.argv)>5): port = int(sys.argv[5])
    if(type(port) != int):
        print "port = %s is not an integer" % port
        usage()

    chunkSize = 10
    if(len(sys.argv)>6): chunkSize = int(sys.argv[6])
    if(type(chunkSize) != int):
        print "chunkSize = %s is not an integer" % port
        usage()

    #run parse-and-insert-many function
    paiManyMongoBall(infile,database,client,collection,port,chunkSize)

if __name__ == "__main__":
    main()
