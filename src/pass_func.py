from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

lines = sc.textFile("README.md")
total = lines.count()

print total

# NOTE: should use persist()
non_blanks = lines.filter(lambda line: line.strip() != "")
text_total = non_blanks.count()
print text_total
