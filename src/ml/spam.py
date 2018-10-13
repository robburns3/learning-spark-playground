from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Spam I Am")
sc = SparkContext(conf = conf)

spam = sc.textFile('spam.txt')
normal = sc.textFile('normal.txt')

# map to email text to a vector of features
tf = HashingTF(numFeatures = 10000)

# Split into words and map to a feature
spam_features = spam.map(lambda email: tf.transform(email.split(' ')))
normal_features = normal.map(lambda email: tf.transform(email.split(' ')))

# create LabeledPoint for positive (spam) and negative (normal) examples
positive_examples = spam_features.map(lambda features: LabeledPoint(1, features))
negative_examples = normal_features.map(lambda features: LabeledPoint(0, features))
training_data = positive_examples.union(negative_examples)

# Cache, since this is iterative
training_data.cache()

# Run regression
model = LogisticRegressionWithSGD.train(training_data)

# Test on examples
POSITIVE_TEST = 'O M G cheap stuff for you send money today to get...'
pos_test = tf.transform(POSITIVE_TEST.split(' '))
print "Prediction for: " + POSITIVE_TEST
print model.predict(pos_test)

NEGATIVE_TEST = 'Hello Hagrid.  I hope Fluffy is feeling better by now...'
neg_test = tf.transform(NEGATIVE_TEST.split(' '))
print "Prediction for: " + NEGATIVE_TEST
print model.predict(neg_test)

