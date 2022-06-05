package ranking.algorithmTraits

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait AlgorithmInterface {
  type T
  def rank(edgesList: T, N: Int, sparkContext: SparkContext): RDD[(Int, Float)]
  def toString() : String
}
