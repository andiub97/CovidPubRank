package ranking

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Map
import scala.math.abs

/** PageRank algorithm.
 *
 *  @param tolerance PageRank algorithm convergence parameter [default 0f].
 *                   With default value PageRank is run for a fixed number of iterations.
 */
class PageRank(val tolerance: Float = 0f) extends RankingAlgorithm {
    type T = RDD[(Int, Int)]
    var context: SparkContext = null;

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


        //val outgoingCnt: RDD[(Int, Iterable[Int])] = edgesList.groupByKey().persist()
        val outEdgesTmp: RDD[(Int, Iterable[Int])] = edgesList.map(edge => (edge._2, edge._1)).groupBy(edge => edge._2).mapValues(_.map(_._1)).persist()
        val mockEdges = this.context.parallelize((0 until N).map(nodeIndex => (nodeIndex, nodeIndex)).toList)
        val mockOutEdges = mockEdges.groupBy(edge => edge._2).mapValues(_.map(_._1)).persist()
        val outgoingCnt = outEdgesTmp.union(mockOutEdges)
        var pr: RDD[(Int, Float)] = outgoingCnt.mapValues(v => 1.0f).persist()


        val maxIter: Int = 10
        val damping: Float = 0.85f

        //Runs PageRank for a fixed number of iterations.
            for (t <- 1 to maxIter) {

                val contributions = outgoingCnt.join(pr).flatMap{
                    case (u, (uLinks, urank)) =>
                        uLinks.map( t => (t, urank/ uLinks.size))
                }

                pr = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15f + 0.85f*v)
        }

        // sort in descending order by PageRank value
        pr.sortBy(- _._2).collect().toList
    }

}
