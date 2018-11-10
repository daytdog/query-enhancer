#!/usr/bin/python

"""
sparkSetup.py
A python class that stores the information needed to set up a
spark environment

Attributes:
    app_name  - name of the spark application environment
                (default: "Testing")
    master    - name of the environment in which spark will run
                (default: "local[*]")
"""

from pyspark import SparkConf,SparkContext

class sparkSetup(object):

    def __init__(self,app_name="Testing",master="local[*]"):
        """ Initializes the Spark setup object
        Args:
            app_name (str or unicode): name of the Spark application
            master (str or unicode): where the master Spark process will
                                     run, and how many cores it will use
        """
        self.app_name = unicode(app_name)
        self.master = unicode(master)

    def setAppName(self,app_name):
        """ Allows the user to set the name of the Spark application
        """
        self.app_name = unicode(app_name)

    def getAppName(self):
        """ Return the name of the Spark application
        """
        return self.app_name

    def getMaster(self):
        """ Return the name of the master environment
        """
        return self.master

    def conf(self):
        """ Configures the Spark app_name and master variables
        """
        return SparkConf().setAppName(self.app_name).setMaster(self.master)

    def sc(self):
        """ Sets up a Spark context environment
        """
        return SparkContext(conf=self.conf())
