package ranking

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.rdd.RDD
import utils.FileUtility


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
   override def rank(graph: T, N: Int): List[(Int, Float)] = {

    val graphRank = graph.pageRank(0.000000001f, 0.15)
    val pr= graphRank.vertices.map(v => (v._1.toInt, v._2.toFloat)).collect().toList
     pr.toList.sortBy(- _._2)

     //graphRank.edges.map(e => ( e.attr.toFloat)).collect().toList
  }
}
