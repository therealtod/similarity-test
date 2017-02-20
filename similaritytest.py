from pyspark import SparkContext, SparkConf
from math import log
from operator import add
import BeautifulSoup as bs

word1 = "mad"
word2 = "angry"


def splitContexts(doc):
	"""This function maps a document into a list of contexts. 
	In this case the context is the single blog post
	"""
	ret = []
	soup = bs.BeautifulSoup(doc)
	divs = soup.findAll('post')
	for div in divs:
		if (div.string):
			ret.append(div.string)
	return ret

def computeHits(ctx):
	"""Maps each context into a tuple of values with the
	following meaning:
	1: number of occurrences of 'word1'
	2: number of occurrences of 'word2'
	3: 1 if both word1 and word2 occur, 0 otherwise
	4: 1 if word1 occurs and word2 doesn't
	5: 1 if word1 doesn't occur and word2 does
	6: 1 if both word1 and word2 don't occur
	7: 1 in any case
	8: the total number of words in this context
	"""
	w1hits = ctx.count(word1)
	w2hits = ctx.count(word2)
	wordstot = len(ctx)
	if w1hits > 0:
		if w2hits > 0:
			return w1hits, w2hits, 1, 0, 0, 0, 1, wordstot
		return w1hits, w2hits, 0, 1, 0, 0, 1, wordstot
	else:
		if w2hits > 0:
			return w1hits, w2hits, 0, 0, 1, 0, 1, wordstot
		return w1hits, w2hits, 0, 0, 0, 1, 1, wordstot

    

def joinGroups(d1, d2):
	return map(add, d1, d2)


def computeSimilarity(hits):
	""" This function computes the G^2 test formula
	using the given data. Hits is tuple which is the bit wise
	sum of the tuples returned by computeHits
	"""
	if 0 in hits:
		return "Not enough information to compute similarity"
	totctx = float (hits[6])
	totwords = float (hits[7])
	px = hits[0] / totwords
	py = hits[1] / totwords
	pnx = 1-px
	pny = 1-py
	pxy = hits[2] / totctx
	pxny = hits[3] / totctx
	pnxy = hits[4] / totctx
	pnxny = hits[5] / totctx
	return pxy * log (pxy/px/py) + pxny * log (pxny/px/pny) + pnxy * log (pnxy/pnx/py) + pnxny * log (pnxny/pnx/pny)

if __name__ == "__main__":
	conf = SparkConf().setAppName("yewno0")
	sc = SparkContext(conf=conf)
	sc.setLogLevel("WARN")
	print "------------------------------------------------------------"
	#Load the dataset to an RDD
	rdd = sc.wholeTextFiles("/home/tod/Downloads/blogs")
	#Split the documents into a list of contexts
	context = rdd.flatMapValues(splitContexts)
	#tokenize the contexts word by word before the following step
	tokenized = context.mapValues(lambda x: x.split(' '))
	#For each context we obtain the information about the occurrence
	#of the given words 
	hits = tokenized.mapValues(computeHits)
	#Group the results by different categories
	hitsByAge = hits.map(lambda (k, val): (
		k.split('.')[2], val)).reduceByKey(joinGroups)
	#Show the results of the computation
	tot = [0, 0, 0, 0, 0, 0, 0, 0]
	for group in hitsByAge.collect():
		tot = map(add, tot, group[1])
		print group[0] + ": " + str(computeSimilarity(group[1]))
	print "General: " + str(computeSimilarity(tot))
	print "------------------------------------------------------------"
