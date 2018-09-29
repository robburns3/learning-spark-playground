from pyspark import SparkConf, SparkContext, StorageLevel

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

nums = sc.parallelize([1,2,4,100])

# persist
nums.persist(StorageLevel.MEMORY_ONLY)

# reduce
total = nums.reduce(lambda x, y: x + y)
print "total is %d" % total

# aggregate
sum_count = nums.aggregate(
    # initial value
    (0,0),  # initial value
    # accumulator object over each value: sum it [0] and count it [1]
    lambda acc,value: (acc[0] + value, acc[1] + 1),
    # merge action for both sum and count
    lambda acc1,acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
)

print "average is %d" % (sum_count[0] / sum_count[1])

