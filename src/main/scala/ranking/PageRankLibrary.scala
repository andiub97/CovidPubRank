package ranking

import org.apache.spark.rdd.RDD
import ranking.algorithmTraits.{AlgorithmInterface, LibraryAlgorithms}


class PageRankLibrary extends AlgorithmInterface with LibraryAlgorithms {

  /**
   * Performs ranking of a graph's nodes via some policy
   *
   * @param graph   graph nodes and edges
   * @param N       number of nodes in the graph
   * */
   override def rank(graph: T, N: Int): RDD[(Int, Float)] ={

     val pagerank = graph.pageRank(0.000000001f, 0.15)
     val pr= pagerank.vertices.map(v => (v._1.toInt, v._2.toFloat))
     pr.sortBy(- _._2)
   }
}
