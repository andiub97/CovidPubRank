package ranking

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import ranking.algorithmTraits.{AlgorithmInterface, NotLibraryAlgorithms}

import scala.collection.immutable.Map

class PageRank() extends AlgorithmInterface with NotLibraryAlgorithms {

  override type T = RDD[(Int, Int)]
    /**
     * Performs ranking of a graph's nodes by using PageRank algorithm
     *
     * @param edgesList list of graph's edges
     * @param N number of nodes in the graph
     **/
    override def rank(edgesList: T, N: Int,sparkContext:SparkContext): RDD[(Int, Float)] = {
      /*
      Get an of outgoing nodes counts for each node (map index is nodeId).
      NOTE: There will never be a node with zero outgoing nodes, during calculation of PageRank.
            This is because when we consider an incoming node B for a node A, B must have at least the link to A.
       */
      val edge = edgesList.collect().toList
      val outgoingCnt: Map[Int, Int] = edge.map(edge => (edge._1, 1)).groupBy(_._1).mapValues(_.map(_._2).sum)
      var pr: Map[Int, Float] = (0 until N).map(nodeIndex => (nodeIndex, 0.15f / N)).toMap

      val maxIter: Int = 10
      val damping: Float = 0.85f

      // Runs PageRank for a fixed number of iterations.

      for (_ <- 1 to maxIter) {
        pr = pr.map { case (nodeId: Int, _: Float) =>
          (nodeId, (1 - damping) / N + damping *
            edge.filter { case (_: Int, dest: Int) => dest == nodeId }
              .map { case (incoming: Int, _: Int) => pr(incoming) / outgoingCnt(incoming) }.sum
          )
        }
      }
      // sort in descending order by PageRank value
      sparkContext.parallelize(pr.toList.sortBy(- _._2))
    }

}