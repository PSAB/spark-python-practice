from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)  

'''SparkContext is the main entrypoint for Spark functionality. SparkSession is used to read Dataframes'''

lines = sc.textFile("ml-100k/u.data")
print(type(lines)) 
# print(lines.collect())

'''RDD: (Resilient Distributed Dataset) a fault-tolerant collection of elements that can be operated on in parallel'''

ratings = lines.map(lambda x: x.split()[2]) # ['3','3','1','2',....]
# print(ratings.collect())
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("{} {}".format(key, value))

'''SPARK SUBMIT: Used to launch applications on a cluster'''
