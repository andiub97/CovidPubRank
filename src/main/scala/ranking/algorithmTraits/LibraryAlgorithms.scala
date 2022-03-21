package ranking.algorithmTraits

import org.apache.spark.graphx.Graph

trait LibraryAlgorithms extends AlgorithmInterface {

  type T = Graph[(Int, String), String]
  override def rank(edgesList: T, N: Int): List[(Int, Float)]
}
