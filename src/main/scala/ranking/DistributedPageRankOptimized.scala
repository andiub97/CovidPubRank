package ranking

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import ranking.algorithmTraits.{AlgorithmInterface, NotLibraryAlgorithms}

class DistributedPageRankOptimized() extends AlgorithmInterface with NotLibraryAlgorithms {


  /**
   * Performs ranking of a graph's nodes by using PageRank algorithm
   *
   * @param edgesList list of graph's edges
   * @param N number of nodes in the graph
   **/
  val parallelism = this.context.getConf.get("spark.default.parallelism").toInt

  override def rank(edgesList: T, N: Int): List[(Int, Float)] = {
    val damping : Float = 0.85f

    val outEdges = edgesList.groupBy(e => e._1).mapValues(_.map(_._2)).partitionBy(new HashPartitioner(parallelism))

    var pageRank: RDD[(Int, Float)] = outEdges.mapValues(_ => 1f / N).partitionBy(new HashPartitioner(parallelism))

    // Runs PageRank until convergence.

    for (_ <- 1 to 10) {
      val nodeSuccessorsScores = outEdges.join(pageRank)
        .flatMap {
          case (_: Int, (nodeSuccessors: List[Int], rank: Float)) =>
            val outDegree = nodeSuccessors.size
            nodeSuccessors.map {
              nodeSuccessor: Int =>
                (nodeSuccessor, rank / outDegree)
            }
        }.partitionBy(new HashPartitioner(parallelism))

      pageRank = nodeSuccessorsScores.reduceByKey((x, y) => x + y)
        .mapValues(score => (1 - damping) / N + damping * score)
    }

    pageRank.sortBy(- _._2).collect().toList
  }

}
