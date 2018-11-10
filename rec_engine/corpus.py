#!/usr/bin/python
#corpusTest.py

import re,sys,os,sparkSetup,jsonTools
import math

def removeQuotes(string):
    """ Remove quotation marks from an input string
    Args:
        string (str): input string that might have the quote "" characters
    Returns:
        str: a string without the quote characters
    """
    return ''.join(i for i in string if i!='"')

def parseDatafileLine(datafileLine):
    """ Parse a line of the data file using the specified regular
        expression pattern
    Args:
        datafileLine (str): input string that is a line from the data file
    Returns:
        str: a string parsed using the given regular expression and without
             the quote characters
    """
    DATAFILE_PATTERN = '^(.+),"(.+)",(.*),(.*),(.*)'
    match = re.search(DATAFILE_PATTERN, datafileLine)
    if match is None:
        print 'Invalid datafile line: %s' % datafileLine
        return (datafileLine, -1)
    elif match.group(1) == '"id"':
        print 'Header datafile line: %s' % datafileLine
        return (datafileLine, 0)
    else:
        product = '%s %s %s' % (match.group(2), match.group(3), match.group(4))
        return ((removeQuotes(match.group(1)), product), 1)

def parseMongoData(mdb_cursor,sc): #no need for a loadMongoData function
    return (sc
            .parallelize(jsonTools.getText(mdb_cursor))
            .cache())

def parseData(filename,sc):
    """ Parse a data file
    Args:
        filename (str): input file name of the data file
    Returns:
        RDD: a RDD of parsed lines
    """
    return (sc
            .textFile(filename, 4, 0)
            .map(parseDatafileLine)
            .cache())

def loadData(path,sc,baseDir,inputPath):
    """ Load a data file
    Args:
        path (str): input file name of the data file
    Returns:
        valid (RDD): a RDD of parsed valid lines
    """
    filename = os.path.join(baseDir, inputPath, path)
    raw = parseData(filename,sc).cache()
    failed = (raw
            .filter(lambda s: s[1] == -1)
            .map(lambda s: s[0]))
    for line in failed.take(10):
        print '%s - Invalid datafile line: %s' % (path, line)
    valid = (raw
            .filter(lambda s: s[1] == 1)
            .map(lambda s: s[0])
            .cache())
    #print '%s - Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (path,
    print '%s - ' % path
    print 'Read %d lines' % raw.count()
    print 'Successfully parsed %d lines' % valid.count()
    print 'Failed to parse %d lines' % failed.count()

    assert failed.count() == 0
    assert raw.count() == (valid.count() + 1)
    return valid

def main(sc):
    baseDir = os.path.join('data')
    inputPath = os.path.join('cs100', 'lab3')

    GOOGLE_PATH = 'Google.csv'
    GOOGLE_SMALL_PATH = 'Google_small.csv'
    AMAZON_PATH = 'Amazon.csv'
    AMAZON_SMALL_PATH = 'Amazon_small.csv'
    GOLD_STANDARD_PATH = 'Amazon_Google_perfectMapping.csv'
    STOPWORDS_PATH = 'stopwords.txt'

    googleSmall = loadData(GOOGLE_SMALL_PATH,sc,baseDir,inputPath)
    google = loadData(GOOGLE_PATH,sc,baseDir,inputPath)
    amazonSmall = loadData(AMAZON_SMALL_PATH,sc,baseDir,inputPath)
    amazon = loadData(AMAZON_PATH,sc,baseDir,inputPath)

    amazonAndGoogle = google + amazon

    #return googleSmall,google,amazonSmall,amazon
    #return amazonAndGoogle
    return amazonSmall

if __name__ == "__main__":
    main()
