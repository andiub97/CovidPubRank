package ranking

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import ranking.algorithmTraits.{AlgorithmInterface, NotLibraryAlgorithms}
import utils.SparkContextSingleton

class DistributedPageRankOptimized() extends AlgorithmInterface with NotLibraryAlgorithms {

  /**
   * Performs ranking of a graph's nodes by using PageRank algorithm
   *
   * @param edgesList list of graph's edges
   * @param N number of nodes in the graph
   **/

  override def rank(edgesList: T, N: Int, sparkContext: SparkContext): RDD[(Int, Float)] = {
    val damping : Float = 0.85f

    val outEdges = edgesList.groupByKey(SparkContextSingleton.DEFAULT_PARALLELISM)

    var pageRank: RDD[(Int, Float)] = outEdges.mapValues(_ => 1f / N)

    // Runs PageRank until convergence.

    for (_ <- 1 to 10) {
      val nodeSuccessorsScores = outEdges.join(pageRank)
        .flatMap {
          case (_: Int, (nodeSuccessors: Iterable[Int], rank: Float)) =>
            val outDegree = nodeSuccessors.size
            nodeSuccessors.map {
              nodeSuccessor: Int =>
                (nodeSuccessor, rank / outDegree)
            }
        }.reduceByKey(_+_).mapValues(score => (1 - damping) / N + damping * score)
      pageRank = nodeSuccessorsScores
      //pageRank = nodeSuccessorsScores.reduceByKey((x, y) => x + y)
       // .mapValues(score => (1 - damping) / N + damping * score)
    }

    pageRank.sortBy(- _._2)
  }

}
