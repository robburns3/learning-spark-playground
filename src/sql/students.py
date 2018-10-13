from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, Row

conf = SparkConf().setMaster("local").setAppName("Spark SQL")
sc = SparkContext(conf = conf)
hc = HiveContext(sc)

# Load JSON data, returns DataFrame:
classes = hc.jsonFile("students.json")
classes.show()

print 'Schema for this table:'
classes.printSchema()

# SELECT
classes.registerTempTable("classes")
harry = hc.sql('SELECT * FROM classes WHERE name = "Harry"') 
harry.show()

# groupBy() returns GroupedData, must perform a grouped operation, like count()
class_counts = classes.groupBy("name").count().collect()

# class_counts is now a list of Row objects
for c in class_counts:
    print "%s # of courses: %d" % (c['name'], c['count'])

# User-Defined Function (UDF)
from pyspark.sql.types import IntegerType
hc.registerFunction("strLen", lambda x: len(x), IntegerType())
nameLens = hc.sql("SELECT name, strLen(name) FROM classes")
print "\nNames and lengths:"
nameLens.show()
