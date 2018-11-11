#!/user/bin/python
"""
enhancedSearch.py
"""

import jsonTools as jt
from mongoBall import mongoBall

import sys,os,datetime
import json
from bson.objectid import ObjectId

def usage():
    print __doc__
    sys.exit()

#enhanced search call
enhancedSearch(searchQueryIn, mdb_collection, stopword_set):
    #figure out how this data is being inserted into the DB
    mdb_captions_query_format = {"tokenized_captions":1}
    mdb_query = mdb_collection.find({}, mdb_captions_query_format)
    query_text = jt.getText(mdb_query)



#main function setup
def main():
    #if the search query input is not given
    if (len(sys.argv)<1): usage()

    #get the search query input (This will be a file)
    searchQueryIn = sys.argv[1]

    #if file name doesn't exist, display usage
    if (os.path.exists(searchQueryIn)==0): usage()

    #following MongoDB info needs to be filled here
    #mongoDB_uri = "<MongoDB_uri here>"
    #groupingId = "<MongoDB grouping id here>"
    #mdb_collection_name = "<MongoDB collection name here>"

    #create the mongoBall and get the collection connector
    mdb = mongoBall()
    mdb_collection = mdb.collection(mdb_message_collection_name)

    #set of stopwords
    stopword_set = set(['all','just','being','over','both','through','yourselves','its','before','herself','had','should','to','only','under','ours','has','do','them','his','very','they','not','during','now','him','nor','did','this','she','each','further','where','few','because','doing','some','are','our','ourselves','out','what','for','while','does','above','between','t','be','we','who','were','here','hers','by','on','about','of','against','s','or','own','into','yourself','down','your','from','her','their','there','been','whom','too','themselves','was','until','more','himself','that','but','don','with','than','those','he','me','myself','these','up','will','below','can','theirs','my','and','then','is','am','it','an','as','itself','at','have','in','any','if','again','no','when','same','how','other','which','you','after','most','such','why','a','off','i','yours','so','the','having','once'])

    #call the enhanced search
    enhancedSearch(searchQueryIn, mdb_collection, stopword_set)

if __name__ == "__main__":
    main()
