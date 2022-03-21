package ranking.algorithmTraits

import org.apache.spark.rdd.RDD

trait NotLibraryAlgorithms extends AlgorithmInterface {

  type T = RDD[(Int, Int)]
  def rank(edgesList: T, N: Int): List[(Int, Float)]
}
