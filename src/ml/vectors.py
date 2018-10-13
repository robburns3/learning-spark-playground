from numpy import array
from pyspark.mllib.linalg import Vectors

# Dense vector:
dense1 = array([1.0, 0.0, 2.0, 0.0])
dense2 = Vectors.dense([1.0, 0.0, 2.0, 0.0])
print "Dense vector: " + str(dense2)

# Sparse vector:
total = 4
sparse1 = Vectors.sparse(total, {0: 1.0, 2: 2.0})
# Or specify with (INDEXES, VALUES)
sparse2 = Vectors.sparse(total, [0, 2], [1.0, 2.0])
print "Equivlent sparse vector: " + str(sparse2)
