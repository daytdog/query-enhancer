#!/usr/bin/python

"""
stopwords.py
A python class that manages a list of stopwords for other modules to use

Attributes:
    filename    - name of the stopwords file
                  (default: "stopwords.txt")
"""

import os

class stopwords(object):

    #def __init__(self,sc,filename="stopwords.txt"):
    def __init__(self,filename="stopwords.txt"):
        """ Initializes the stopwords object
        Args:
            sc (SparkContext): a Spark context environment
            filename (str or unicode): name of the text file that
                                       contains the list of stop words
        """
        #self.sc = sc
        self.filename = filename

        #set the path to stopwords.txt
        self.stopfile = os.path.join(os.path.dirname(os.path.abspath(__file__)),self.filename)
        #grab the stopwords from stopwords.txt and put them in a set
        #create a backup set of stopwords in case the stopwords list needs to be reset
        self.stopwords_reset_set = set(['all','just','being','over','both','through','yourselves','its','before','herself','had','should','to','only','under','ours','has','do','them','his','very','they','not','during','now','him','nor','did','this','she','each','further','where','few','because','doing','some','are','our','ourselves','out','what','for','while','does','above','between','t','be','we','who','were','here','hers','by','on','about','of','against','s','or','own','into','yourself','down','your','from','her','their','there','been','whom','too','themselves','was','until','more','himself','that','but','don','with','than','those','he','me','myself','these','up','will','below','can','theirs','my','and','then','is','am','it','an','as','itself','at','have','in','any','if','again','no','when','same','how','other','which','you','after','most','such','why','a','off','i','yours','so','the','having','once'])
        self.stopwords_set = self.stopwords_reset_set
    def update_stopfile(self):
        """ Update the existing stopword text file with the set of
            stopwords, stopwords_set
        """
        with open(self.stopfile, 'w') as myfile:
            for stopword in self.stopwords_set:
                myfile.write(unicode(stopword) + "\n")

    def addStopword(self,newWord):
        """ Add a new word to the existing set of stopwords and update
            both the set and text file
        Args:
            newWord (str or unicode): word to be added to the stopwords
                                      set and text file
        """
        self.stopwords_set.add(unicode(str(newWord).lower()))
        self.update_stopfile()

    def removeStopword(self,removeWord):
        """ Remove a word from the existing set of stopwords and update
            both the set and text file
        Args:
            removeWord (str or unicode): word to be removed from the
                                         stopwords set and text file
        """
        self.stopwords_set.remove(unicode(str(removeWord).lower()))
        self.update_stopfile()

    def reset_stopwords(self):
        """ Reset the stopwords set and text file to the most basic set
            of stopwords
        """
        self.stopwords_set = self.stopwords_reset_set
        self.update_stopfile()
