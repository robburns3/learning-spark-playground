import org.apache.spark._
import org.apache.spark.SparkContext._


object WordCount {
def main(args: Array[String]) {

val conf = new SparkConf().setAppName("wordCount")
val sc = new SparkContext(conf)

val inputFile = "README.md"
val input = sc.textFile(inputFile)
val words = input.flatMap(line => line.split(" "))
val counts = words.map(word => (word, 1)).reduceByKey{case (x,y) => x + y}

val outputFile = "output"
counts.saveAsTextFile(outputFile)
}
}
