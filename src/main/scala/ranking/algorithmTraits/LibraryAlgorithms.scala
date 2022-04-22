package ranking.algorithmTraits

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

trait LibraryAlgorithms extends AlgorithmInterface {

  type T = Graph[(Int, String), String]
  override def rank(edgesList: T, N: Int): RDD[(Int, Float)]
}
