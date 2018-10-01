import json, collections, os, shutil
from pyspark import SparkConf, SparkContext

def printKV(rdd, max_lines=None, message=None) :
    """Helper for printing out RDD key, value pairs"""
    print
    if message:
        print message
    line = 0
    if isinstance(rdd, dict):
        print rdd
    else:
        for k, v in rdd:
            if line == max_lines:
                break
            items = v
            if isinstance(v, collections.Iterable) and not isinstance(v, str):
                items = [val for val in v]
            print k + ":" + str(items)
            line += 1

# Initialize
conf = SparkConf().setMaster("local").setAppName("Key Value")
sc = SparkContext(conf = conf)

# Some useful data
values = [("a", 5), ("b", 20), ("a", 10), ("c", 15), ("b", 2)]


# Create Key Value
pairs = sc.parallelize(values)
pairs.persist()  # re-used later
printKV(pairs.groupByKey().collect(), 5, "Create and Group KV")


# Word Count
lines = sc.textFile("README.md")
words = lines.flatMap(lambda x: x.split(" "))
result = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+ y)
printKV(result.collect(), 5, "Word Count")


# combineByKey Average
sumCount = pairs.combineByKey(
    # new key in partition? initial value (key, count)    
    lambda value: (value, 1),
    # existing key in partition? merge
    lambda x, value: (x[0] + value, x[1] + 1),
    # existing key in partition? merge
    lambda x, y: (x[0] + y[0], x[1] + y[1])
)
result = sumCount.map(lambda (key, xy): (key, xy[0]/xy[1]))
printKV(result.collectAsMap(), message="combineByKey Average")


# Joins
scores = sc.parallelize([("Rockets", 20), ("Talons", 30), ("Waves", 40)])
players = sc.parallelize([("Rockets", "Sean"), ("Rockets", "Phil"), ("Waves", "Selena"), ("Gliders", "Tom")])
printKV(scores.join(players).collectAsMap(), message="Join, Inner")
printKV(scores.leftOuterJoin(players).collectAsMap(), message="Join, Left Outer")
printKV(scores.rightOuterJoin(players).collectAsMap(), message="Join, Right Outer")


# Sav JSONe
playerOutDir = 'player-data-output'
if os.path.exists(playerOutDir):
    shutil.rmtree(playerOutDir)
players.map(lambda x: json.dumps(x)).saveAsTextFile(playerOutDir)
print "\nSave JSON to: " + playerOutDir


