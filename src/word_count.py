from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

lines = sc.textFile("README.md")
words = lines.flatMap(lambda line: line.split())

total = words.count()

print total
# persist() !
print words.first()
