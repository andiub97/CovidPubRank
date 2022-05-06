package ranking

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import ranking.algorithmTraits.{AlgorithmInterface, NotLibraryAlgorithms}

class DistributedPageRank() extends AlgorithmInterface with NotLibraryAlgorithms {

val parallelism = this.context.getConf.get("spark.default.parallelism").toInt



    /**
     * Performs ranking of a graph's nodes by using PageRank algorithm
     *
     * @param edgesList list of graph's edges
     * @param N number of nodes in the graph
     **/

    override def rank(edgesList: T, N: Int): RDD[(Int, Float)] = {
        val damping : Float = 0.85f

        val outEdgesTmp: RDD[(Int, Iterable[Int])] = edgesList.map(edge => (edge._2, edge._1)).groupBy(edge => edge._2).mapValues(_.map(_._1))
        val mockEdges = this.context.parallelize((0 until N).map(nodeIndex => (nodeIndex, nodeIndex)).toList,parallelism)
        val mockOutEdges = mockEdges.groupBy(edge => edge._2).mapValues(_.map(_._1))
        val outEdges = outEdgesTmp.union(mockOutEdges).persist()
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
              }//.partitionBy(new HashPartitioner(parallelism))

            pageRank = nodeSuccessorsScores.reduceByKey((x, y) => x + y)
              .mapValues(score => (1 - damping) / N + damping * score)
        }

        pageRank.sortBy(- _._2)
    }

}
