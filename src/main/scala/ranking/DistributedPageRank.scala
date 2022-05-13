package ranking

import org.apache.spark.{HashPartitioner, RangePartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import ranking.algorithmTraits.{AlgorithmInterface, NotLibraryAlgorithms}
import utils.SparkContextSingleton

class DistributedPageRank() extends AlgorithmInterface with NotLibraryAlgorithms {

    /**
     * Performs ranking of a graph's nodes by using PageRank algorithm
     *
     * @param edgesList list of graph's edges
     * @param N number of nodes in the graph
     **/

    override def rank(edgesList: T, N: Int, sparkContext: SparkContext): RDD[(Int, Float)] = {
        val damping : Float = 0.85f

        val outEdgesTmp = edgesList.groupByKey(SparkContextSingleton.DEFAULT_PARALLELISM)
        val mockEdges = sparkContext.parallelize((0 until N).map(nodeIndex => (nodeIndex, nodeIndex)))
        val mockOutEdges = mockEdges.groupByKey(SparkContextSingleton.DEFAULT_PARALLELISM)
        val outEdges = outEdgesTmp.union(mockOutEdges).persist((StorageLevel.MEMORY_AND_DISK))
        var pageRank: RDD[(Int, Float)] = outEdges.mapValues(_ => 1f / N).partitionBy(new RangePartitioner(SparkContextSingleton.DEFAULT_PARALLELISM, outEdges)).persist(StorageLevel.MEMORY_AND_DISK)

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
        }

        pageRank.sortBy(- _._2)
    }

}
