#!/usr/bin/python
#test_tfidf.py

#in order for this program to work, it must be run when in the
#Spark directory with data/cs100/lab3 as a subdirectory

import sparkSetup
import corpusTest as cor
import textAnalyzer as ta

#setup spark config
sc = sparkSetup.sparkSetup().sc()

#grab RDDs of googleSmall, google, amazonSmall, and amazon
#googleSmall,google,amazonSmall,amazon = cor.main(sc)
google,amazon = cor.main(sc)

"""
Working on the small Google and Amazon datasets
"""
#tokenize both the amazonSmall and googleSmall RDDs
amazonRecToToken = amazonSmall.map(lambda x: (x[0], ta.tokenize(x[1])))
googleRecToToken = googleSmall.map(lambda x: (x[0], ta.tokenize(x[1])))
print 'Amazon small dataset is %s products, Google small dataset is %s products' % (amazonRecToToken.count(), googleRecToToken.count())

#grab the total number of tokens in amazonSmall and googleSmall
totalTokens = ta.countTokens(amazonRecToToken) + ta.countTokens(googleRecToToken)
print 'Total tokens in the small Amazon and Google dataset is %s' % (totalTokens)

#create a pair RDD called corpusRDD, consisting of a combination
#of amazonRecToToken and googleRecToToken. Each element of the
#corpusRDD should be a pair consisting of a key from one of the
#small datasets (ID or URL) and the value is the associated
#value for that key from the small datasets.
corpusRDD = amazonRecToToken + googleRecToToken

#takes the inverse document frequency of all words found
#and saves the words and their weights in an RDD
idfsSmall = ta.inverseDocumentFrequency(corpusRDD)

#print out the 11 tokens with the smallest IDF in small dataset
smallIDFTokens = idfsSmall.takeOrdered(11, lambda s: s[1])
print smallIDFTokens

#print out the 11 tokens with the largest IDF in small dataset
largeIDFTokens = idfsSmall.takeOrdered(11, lambda s: -s[1])
print largeIDFTokens


#plot a histogram of IDF values
import matplotlib.pyplot as plt
small_idf_values = idfsSmall.map(lambda s: s[1]).collect()
fig = plt.figure(figsize=(8,3))
plt.hist(small_idf_values, 50, log=True)
pass


#save the sets of [('word', weight)] as a dictionary
idfsSmallWeights = idfsSmall.collectAsMap()

#grab a document to test TF-IDF on
docb000hkgj8k = amazonRecToToken.filter(lambda x: x[0] == 'b000hkgj8k').collect()
print docb000hkgj8k
#grab the tokens of the document
recb000hkgj8k = docb000hkgj8k[0][1]
print recb000hkgj8k


#run TF-IDF on the test document tokens
rec_b000hkgj8k_weights = ta.tfidf(recb000hkgj8k, idfsSmallWeights)
print 'Amazon record "b000hkgj8k" has tokens and weights:\n%s' % rec_b000hkgj8k_weights


"""
Working on the full Google and Amazon datasets
"""
#full dataset tokenization
amazonFullRecToToken = amazon.map(lambda rec: (rec[0], ta.tokenize(rec[1])))
googleFullRecToToken = google.map(lambda rec: (rec[0], ta.tokenize(rec[1])))
print 'Amazon full dataset is %s products, Google full dataset is %s products' % (amazonFullRecToToken.count(), googleFullRecToToken.count())

#create a full corpus RDD and grab the IDF RDD from it
fullCorpusRDD = amazonFullRecToToken + googleFullRecToToken
idfsFull = ta.inverseDocumentFrequency(fullCorpusRDD)
idfsFullCount = idfsFull.count()
print 'There are %s unique tokens in the full datasets.' % idfsFullCount

#recompute the IDFs for the full dataset and broadcast it
idfsFullWeights = idfsFull.collectAsMap()
idfsFullBroadcast = sc.broadcast(idfsFullWeights)

#pre-compute TF-IDF weights. Build mappings from record
#ID weight vector
amazonWeightsRDD = amazonFullRecToToken.map(lambda rec: (rec[0],ta.tfidf(rec[1],idfsFullBroadcast.value)))
googleWeightsRDD = googleFullRecToToken.map(lambda rec: (rec[0],ta.tfidf(rec[1],idfsFullBroadcast.value)))
print 'There are %s Amazon weights and %s Google weights.' % (amazonWeightsRDD.count(),googleWeightsRDD.count())

#compute norms for the weights from the full datasets
amazonNorms = amazonWeightsRDD.map(lambda (url,weights): (url, ta.norm(weights)))
amazonNormsBroadcast = sc.broadcast(amazonNorms.collect())
googleNorms = googleWeightsRDD.map(lambda (url, weights): (url, ta.norm(weights)))
googleNormsBroadcast = sc.broadcast(googleNorms.collect())

#create inverted indices from the full datasets
amazonInvPairsRDD = (amazonWeightsRDD.flatMap(lambda rec: ta.invert(rec)).cache())
googleInvPairsRDD = (googleWeightsRDD.flatMap(lambda rec: ta.invert(rec)).cache())
print 'There are %s Amazon inverted pairs and %s Google inverted pairs.' % (amazonInvPairsRDD.count(),googleInvPairsRDD.count())
print amazonInvPairsRDD.take(10)

#identify common tokens from the full dataset
commonTokens = (amazonInvPairsRDD.join(googleInvPairsRDD).map(lambda rec: ta.swap(rec)).groupByKey().map(lambda rec: (rec[0], list(rec[1]))).cache())
print 'Found %d common tokens' % commonTokens.count()

#identify common tokens from the full dataset with
#fast cosine similarity
amazonWeightsBroadcast = sc.broadcast(amazonWeightsRDD.collectAsMap())
googleWeightsBroadcast = sc.broadcast(googleWeightsRDD.collectAsMap())
similaritiesFullRDD = (commonTokens.map(lambda record: ta.fastCosineSimilarity(record,amazonWeightsBroadcast,googleWeightsBroadcast,amazonNormsBroadcast,googleNormsBroadcast)).cache())
similaritiesFullRDD.count()

#grab all matches to the amazon ID 'b00005lzly'
similarityTest = similaritiesFullRDD.filter(lambda ((aID, gURL), cs): aID == 'b00005lzly').collect()
sorted(similarityTest, key=lambda similarity: similarity[1])   #ascending
sorted(similarityTest, key=lambda similarity: -similarity[1])  #decending

#grab top google match and take a look at the contents
amazon.filter(lambda (aID,val): aID == 'b00005lzly').collect()
google.filter(lambda (gURL,val): gURL == 'http://www.google.com/base/feeds/snippets/13393440070032836115').collect()
#notice that both are about the "employee appraiser deluxe 5.0"

"""
Analysis
"""
#counting true positives, false positives, and false negatives
simsFullRDD = similaritiesFullRDD.map(lambda x: ("%s %s" % (x[0][0], x[0][1]), x[1]))
simsFullValuesRDD = (simsFullRDD.map(lambda x: x[1]).cache())



#searching by document
document = amazon.filter(lambda (aID,val): aID == 'b00029bqa2')
documentRecToToken = document.map(lambda rec: (rec[0], ta.tokenize(rec[1])))

finalCorpusRDD = documentRecToToken + fullCorpusRDD
idfsFinal = ta.inverseDocumentFrequency(finalCorpusRDD)

idfsFinalWeights = idfsFinal.collectAsMap()
idfsFinalBroadcast = sc.broadcast(idfsFinalWeights)

documentWeightsRDD = documentRecToToken.map(lambda rec: (rec[0],ta.tfidf(rec[1],idfsFinalBroadcast.value)))
fullWeightsRDD = fullCorpusRDD.map(lambda rec: (rec[0],ta.tfidf(rec[1],idfsFinalBroadcast.value)))

documentNorms = documentWeightsRDD.map(lambda (url,weights): (url, ta.norm(weights)))
documentNormsBroadcast = sc.broadcast(documentNorms.collect())
fullNorms = fullWeightsRDD.map(lambda (url, weights): (url, ta.norm(weights)))
fullNormsBroadcast = sc.broadcast(fullNorms.collect())

documentInvPairsRDD = (documentWeightsRDD.flatMap(lambda rec: ta.invert(rec)).cache())
fullInvPairsRDD = (fullWeightsRDD.flatMap(lambda rec: ta.invert(rec)).cache())

newCommonTokens = (documentInvPairsRDD.join(fullInvPairsRDD).map(lambda rec: ta.swap(rec)).groupByKey().map(lambda rec: (rec[0], list(rec[1]))).cache())

documentWeightsBroadcast = sc.broadcast(documentWeightsRDD.collectAsMap())
fullWeightsBroadcast = sc.broadcast(fullWeightsRDD.collectAsMap())

similaritiesFinalRDD = (newCommonTokens.map(lambda record: ta.fastCosineSimilarity(record,documentWeightsBroadcast,fullWeightsBroadcast,documentNormsBroadcast,fullNormsBroadcast)).cache())

similarityFinalTest = similaritiesFinalRDD.filter(lambda ((aID, gURL), cs): aID == 'b00029bqa2').collect()

sorted(similarityFinalTest, key=lambda similarity: similarity[1])

#original document
document.filter(lambda (aID,val): aID == 'b00029bqa2').collect()
#top 3
google.filter(lambda (aID,val): aID == 'http://www.google.com/base/feeds/snippets/13801903643027621522').collect()
google.filter(lambda (aID,val): aID == 'http://www.google.com/base/feeds/snippets/18431790997623472871').collect()
google.filter(lambda (aID,val): aID == 'http://www.google.com/base/feeds/snippets/10688959294324458483').collect()
