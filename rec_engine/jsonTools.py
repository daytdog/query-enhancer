#!/usr/bin/python
#jsonTools.py

#Custom Error Exception
class CustomError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def getGroup(json_doc):
    """ Determines which group a json document belongs to
    Args:
        json_doc (dict): a json document
    Returns:
        group (string): the name of the group the document belongs to
    """
    if "source" in json_doc:
        #using hard-coded "source" values from 'Atlas BMC ITSM Mappings v1' Excel document
        json_source = str(json_doc["source"])

        #determine group
        if json_source == "BMC Remedy Incident Management":
            return "incidents"
        elif json_source == "BMC Remedy Change Management":
            return "changes"
        elif json_source == "BMC Remedy Problem Management":
            return "problems"
        elif json_source == "BMC Remedy Atrium CMDB":
            if "classType" in json_doc:
                return "cic"
            elif "parentType" in json_doc:
                return "cir"
            else:
                return "cis"
        elif json_source == "BMC Remmedy Common Foundation":
            if str(json_doc["type"]) == "person":
                return "people"
            elif str(json_doc["type"]) == "support":
                return "groups"
        else:
            return "misc"

    else:
        #if "source" key doesn't exist, raise an error
        raise CustomError("Dictionary key \"source\" not found in json document")

def returnFields_incidents(incidents_collection):
    #return the value of the summary field for all documents in the collection
    #Need to grab the summary field, details field, workNotes.summary field,
    #and workNotes.details field
    return incidents_collection.find({},{"summary":1, "details":1, "workNotes.details":1, "workNotes.summary"    :1, "_id":1})

#http://stackoverflow.com/questions/34889689/obtaining-values-from-a-list-dictionary-hybrid-list-dynamically

def recFetcher(obj):
    """Recursive function which can print the values,
    only if they are neither a list nor a dictionary
    Args:
        obj (obj): Any object, a list of values taken from a MongoDB query in this case
    Returns:
        obj (not list or dict): an object that is neither a list nor a dictionary, a string(?)
    """
    if isinstance(obj, list):       #if the object is a list
        for item in obj:            #go into the list
            for value in recFetcher(item):      #run recFetcher recursively
                yield value         #yields the value given by the recFetcher recursion
    elif isinstance(obj, dict):     #if the object is a dictionary
        for item in obj:            #for the items in the dictionary
            for value in recFetcher(obj[item]): #run the items through recFetcher recursively
                yield value         #yields the value given by the recFetcher recursion
    else:
        yield obj                   #else, just yield the final object


def parseMongoRecord(mdb_record):
    """ Expects a record containing an '_id' key, and other
    important non-selection field keys such as summary,
    details, etc.
    Args:
        mdb_record (dictionary): a record returned by a MongoDB query
    Returns
        (record_id,record_str): an id-string touple of the MongoDB query record
    """
    record_id = mdb_record.pop('_id', None)
    record_str = " ".join(list(recFetcher(mdb_record)))
    return (record_id, record_str)

def getText(mdb_cursor): #timeit() avg time for 116 records: 5.55 ms
    """Creates a list of (record_id,record_str) tuples from a given MongoDB query
    Args:
        mdb_cursor (obj): a MongoDB query containing an '_id' key, and other
                          important non-selection field keys such as summary,
                          details, etc.
    Returns:
        corpus (list):    a corpus containing a list of (id,str) tuples
    """
    corpus = []
    for record in mdb_cursor:
        corpus.append(parseMongoRecord(record))
    #Note: Dictionaries are unordered, so the values from dictionary may
    #not be in the same position, as in the literal passed
    return corpus

