"""
Accumulators: shared variables from workers ==> driver; write-only
              e.g., counting, debugging
Broadcast Variables: propogated variables for common lookups or operations
              e.g., open DB connections, share a lookup table
"""
from pyspark import SparkConf, SparkContext

# Initialize
conf = SparkConf().setMaster("local").setAppName("Advanced Programming")
sc = SparkContext(conf = conf)

# Accumulator
values = [1, 2, 5, 3, 3, 6, 2, 4]
evens = sc.accumulator(0)
nums = sc.parallelize(values)
def count_evens(num):
    global evens
    if num % 2 == 0:
        evens += 1
    return num
nums.map(count_evens).collect()
print "Total even numbers: %d" % evens.value
print

# Broadcast Variables
shared_list = [2, 3]
share = sc.broadcast(shared_list)
def use_broadcast_share(num):
    if num in share.value:
        print "Matches broadcast: %d" % num
    return num
nums.map(use_broadcast_share).collect()
print


