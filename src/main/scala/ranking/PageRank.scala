package ranking

import scala.collection.immutable.Map

class PageRank(val tolerance: Float) extends RankingAlgorithm {
    type T = List[(Int, Int)]

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
      /*if (tolerance > 0f) {
        var oldPr: Map[Int, Float] = (0 until N).map(nodeIndex => (nodeIndex, 0f)).toMap
        var maxDiff: Float = 10f

        do {
          oldPr = pr
          pr = pr.map { case (nodeId: Int, nodePr: Float) =>
            (nodeId, (1 - damping) / N + damping *
              edgesList.filter { case (incoming: Int, dest: Int) => dest == nodeId }
                .map { case (incoming: Int, dest: Int) => pr(incoming) / outgoingCnt(incoming) }.sum
            )
          }

          maxDiff = pr.map { case (nodeId: Int, nodePr: Float) =>
            (nodeId, abs(nodePr - oldPr(nodeId)))
          }.maxBy(_._2)._2

        } while (maxDiff > tolerance)

      }

      //Runs PageRank for a fixed number of iterations.
      else {*/
        for (t <- 1 to maxIter) {
          // pr => (nodeId, pr(nodeId))
          pr = pr.map { case (nodeId: Int, nodePr: Float) =>
            (nodeId, (1 - damping) / N + damping *
              edgesList.filter { case (incoming: Int, dest: Int) => dest == nodeId }
                .map { case (incoming: Int, dest: Int) => pr(incoming) / outgoingCnt(incoming) }.sum
            )
          }
        //}
      }


      // sort in descending order by PageRank value
      pr.toList.sortBy(- _._2)
    }

}