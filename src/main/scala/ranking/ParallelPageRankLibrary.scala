package ranking

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeTriplet, Graph, Pregel, VertexId}

class ParallelPageRankLibrary extends RankingAlgorithm {
  override type T = Graph[(Int, String), String]
  var context: SparkContext = null


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

    val resetProb = 0.15
    val pagerankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
        (vid, vdata, deg) => deg.getOrElse(0)
      }
      // Set the weight on the edges based on the degree
      .mapTriplets(e => 1.0 / e.srcAttr)
      // Set the vertex attributes to the initial pagerank values
      .mapVertices((id, attr) => 0.15)

    def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double = {
      resetProb/N + (1.0 - resetProb) * msgSum
    }

    def sendMessage(edge: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] =
      Iterator((edge.dstId, edge.srcAttr * edge.attr))
    def messageCombiner(a: Double, b: Double): Double = a + b
    val initialMessage = 0.0
    // Execute Pregel for a fixed number of iterations.
    val pr = Pregel(pagerankGraph, initialMessage, 10)(
      vertexProgram, sendMessage, messageCombiner)

    val res = pr.vertices.map(v => (v._1.toInt, v._2.toFloat))

    res.collect().toList.sortBy(- _._2)
  }
}
