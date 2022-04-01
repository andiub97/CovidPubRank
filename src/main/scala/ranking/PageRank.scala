package ranking

import ranking.algorithmTraits.{AlgorithmInterface, ListAlgorithms}

import scala.collection.immutable.Map

class PageRank() extends AlgorithmInterface with ListAlgorithms {

    /**
     * Performs ranking of a graph's nodes by using PageRank algorithm
     *
     * @param edgesList list of graph's edges
     * @param N number of nodes in the graph
     **/
    override def rank(edgesList: T, N: Int): List[(Int, Float)] = {
      /*
      Get an of outgoing nodes counts for each node (map index is nodeId).
      NOTE: There will never be a node with zero outgoing nodes, during calculation of PageRank.
            This is because when we consider an incoming node B for a node A, B must have at least the link to A.
       */
      val outgoingCnt: Map[Int, Int] = edgesList.map(edge => (edge._1, 1)).groupBy(_._1).mapValues(_.map(_._2).sum)
      var pr: Map[Int, Float] = (0 until N).map(nodeIndex => (nodeIndex, 0.15f / N)).toMap

      val maxIter: Int = 10
      val damping: Float = 0.85f

      // Runs PageRank until convergence.

      for (_ <- 1 to maxIter) {
        pr = pr.map { case (nodeId: Int, _: Float) =>
          (nodeId, (1 - damping) / N + damping *
            edgesList.filter { case (_: Int, dest: Int) => dest == nodeId }
              .map { case (incoming: Int, _: Int) => pr(incoming) / outgoingCnt(incoming) }.sum.toFloat
          )
        }
      }
      // sort in descending order by PageRank value
      pr.toList.sortBy(- _._2)
    }

}