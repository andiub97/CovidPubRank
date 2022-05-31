package ranking.algorithmTraits

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait NotLibraryAlgorithms extends AlgorithmInterface {

  override type T = RDD[(Int, Int)]
  override def rank(edgesList: T, N: Int, sparkContext: SparkContext): RDD[(Int, Float)]
}
