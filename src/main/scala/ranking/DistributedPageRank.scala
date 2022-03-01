package ranking

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import utils.SparkContextSingleton

import scala.collection.immutable.Map
import scala.math.abs

/** PageRank algorithm.
 *
 *  @param tolerance PageRank algorithm convergence parameter [default 0f].
 *                   With default value PageRank is run for a fixed number of iterations.
 */
class DistributedPageRank() extends RankingAlgorithm {
    type T = RDD[(Int, Int)]
    var context: SparkContext = SparkContextSingleton.getContext();

    def setContext(sc: SparkContext) = {
        this.context = sc
    }
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


        val outEdgesTmp: RDD[(Int, Iterable[Int])] = edgesList.groupBy(edge => edge._1).mapValues(_.map(_._2)).persist()
        val mockEdges = this.context.parallelize((0 until N).map(nodeIndex => (nodeIndex, nodeIndex)).toList)
        val mockOutEdges = mockEdges.groupBy(edge => edge._2).mapValues(_.map(_._1)).persist()
        val links = outEdgesTmp.union(mockOutEdges)

        var ranks: RDD[(Int, Float)] = links.mapValues(v => 1f / N).persist()

        val maxIter: Int = 10

        //Runs PageRank for a fixed number of iterations.
        for (i <- 1 to maxIter) {

            val contributions = links.join(ranks).flatMap {
                case (u, (uLinks: List[Int], urank)) =>
                    uLinks.map(t =>
                        if (t == u) (t, 0f)
                        else (t, urank / uLinks.size))

            }
            ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => (0.15f / N) + 0.85f * v)
        }

        // sort in descending order by PageRank value
        ranks.sortBy(-_._2).collect().toList
    }

}
