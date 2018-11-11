#!/user/bin/python
"""
jsonTools.py
"""

def recFetcher(obj):
    """
    Recursive function which can print the values,
    only if they are neither a list nor a dictionary
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
    """
    Expects a record containing an '_id' key, and other
    important non-selection field keys such as summary,
    details, etc.
    """
    record_id = mdb_record.pop('_id', None)
    record_str = " ".join(list(recFetcher(mdb_record)))
    return (record_id, record_str)


def getText(mdb_cursor): #timeit() avg time for 116 records: 5.55 ms
    """
    Creates a list of (record_id,record_str) tuples from a given MongoDB query
    """
    corpus = []
    for record in mdb_cursor:
        corpus.append(parseMongoRecord(record))
    #Note: Dictionaries are unordered, so the values from dictionary may
    #not be in the same position, as in the literal passed
    return corpus
