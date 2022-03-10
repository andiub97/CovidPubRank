package ranking

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph


class PageRankLibrary extends RankingAlgorithm {
  type T = Graph[(Int, String), String]
  var context: SparkContext = null;

  def setContext(sc: SparkContext) = {
    this.context = sc
  }
  /**
   * Performs ranking of a graph's nodes via some policy
   *
   * @param edgesList list of graph's edges
   * @param N         number of nodes in the graph
   * */
   override def rank(graph: T, N: Int)={

     val pagerank = graph.pageRank(0.000000001f, 0.15)
     val pr= pagerank.vertices.map(v => (v._1.toInt, v._2.toFloat)).collect().toList
     pr.toList.sortBy(- _._2)
   }
}
