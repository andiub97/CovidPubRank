package ranking.algorithmTraits

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.SparkContextSingleton

trait AlgorithmInterface {
  type T
  def rank(edgesList: T, N: Int, sparkContext: SparkContext): RDD[(Int, Float)]
}
