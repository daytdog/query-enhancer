#!/usr/bin/python

"""
jsonTextParse.py
"""

import os,sys,json
import textAnalyzer as ta
import numpy as np

def usage():
    print __doc__
    sys.exit()

def text2json(infile):
    """ Turns a text file containing json documents and converts
        it into a python list of json documents
    Args:
        infile (str or unicode): a text file that contains a single
                                 json-formatted document on each line
    Returns:
        data (list): a list of json-formatted documents
    """
    data = []

    with open(infile) as f:
        for line in f:
            data.append(json.loads(line))

    return data

def idunno(jsonDoc):
    """ Prints a list of keys from a json document
    Args:
        jsonDoc (dict): a json document
    """
    keys = []
    for key in jsonDoc:
        keys.append(key)

    for i in np.arange(len(keys)):
        if (type(jsonDoc[keys[i]]) == (unicode or str)):
            if (len(ta.tokenize(jsonDoc[keys[i]])) >= 3):
                print "%s: \t%s" % (keys[i], jsonDoc[keys[i]])

def getKeyPairs(jsonDoc):
    """ Prints a list of keys from a json document
    Args:
        jsonDoc (dict): a json document
    """
    keys = []
    for key in jsonDoc:
        keys.append(key)

    for i in np.arange(len(keys)):
        if (type(jsonDoc[keys[i]]) == list):
            listParser(keys[i],jsonDoc[keys[i]])
        else:
            print "%s: \t%s" % (keys[i], jsonDoc[keys[i]])

def listParser(keyName,inList):
    """ Prints the contents of a list that a keyName points to. If the
        list contains dictionaries, then those subkeys and their con-
        tents will also be printed. keyName.subKey: subKeyContents
    Args:
        keyName (str or unicode): Name of a key that points to a list
        inList (list): List that the keyName points to
    """
    for i in np.arange(len(inList)):
        if(type(inList[i]) == dict): #if the list contains dictionaries
            subset = []
            for sub in inList[i]:
                subset.append(sub)
            for j in np.arange(len(subset)):
                print "%s.%s: \t%s" % (keyName, subset[j], inList[i][subset[j]])
        else:
            print "%s: \t%s" % (keyName, inList[i])

def getKeys(jsonDoc):
    """ Generate a list of keys from a json document
    Args:
        jsonDoc (dict): a json document
    Returns:
        keys (list): a list of keys containted in the json doc
    """
    keys = []
    for key in jsonDoc:
        if (type(jsonDoc[key]) != list):
            keys.append(key)
        #if the current key points to a list, return a key-subkey pair
        else:
            keys = keyListParser(key,jsonDoc[key],keys)
    return keys

def keyListParser(keyName,inList,keysList):
    """ Updates a list of keys and key-subkey pairs with new
        key-subkey pairs
    Args:
        keyName (str or unicode): Name of a key that points to a list
        inList (list): List that the keyName points to
        keysList (list): a list of keys and key-subkey pairs containted
                         in the json doc
    Returns:
        keysList (list): a list of keys and key-subkey pairs containted
                         in the json doc
    """
    for i in np.arange(len(inList)):
        if(type(inList[i]) == dict):
            for sub in inList[i]:
                keysList.append([keyName,i,sub])
        else:
            keysList.append([keyName,i])
    return keysList

def getSubListValue(jsonDoc,subList):
    """ Takes a list of sub-keys and returns the value of the jsonDoc
        for the lowest-level sub-key of the list
    Args:
        jsonDoc (dict): a json document
        subList (list): a list of sub-keys
    Returns:
        value (str or unicode): value of the lowest-level sub-key
                                in subList
    """
    value = jsonDoc
    #traverse down the sub-key pair and grab the value at the bottom
    for item in subList:
        value = value[item]
    return value

def returnValues(jsonDoc):
    """ Generates both a list of all keys and sub-key paris found
        in a json document and a list of their values.
    Args:
        jsonDoc (dict): a json document
    Returns:
        keyList (list): a list of key and sub-key pairs
        values (list): a list of values corresponding to the keys
                       and sub-key pairs found in the json document
    """
    keyList = getKeys(jsonDoc)
    values = []
    for key in keyList:
        if (type(key) == list):
            values.append(getSubListValue(jsonDoc,key))
        else:
            values.append(jsonDoc[key])
    return keyList,values

def main():
    if (len(sys.argv)<2): usage()

    infile = sys.argv[1]
    if (os.path.exists(infile)==0): usage()

    dataset = text2json(infile)
    for set in dataset:
        getKeyPairs(set)
        #idunno(set)
        print ''

if __name__ == "__main__":
    main()
