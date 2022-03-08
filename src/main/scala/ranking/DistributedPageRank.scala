package ranking

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.SparkContextSingleton

class DistributedPageRank() extends RankingAlgorithm {
    type T = RDD[(Int, Int)]
    var context: SparkContext = SparkContextSingleton.getContext()

    def setContext(sc: SparkContext): Unit = {
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
        val links = edgesList.groupBy(e => e._1).mapValues(_.map(_._2))

        var ranks: RDD[(Int, Float)] = context.parallelize((0 until N).map(n => (n, 0.15f / N)))

        val maxIter: Int = 10


        //Runs PageRank for a fixed number of iterations.
        for (_ <- 1 to maxIter) {


            val contributions = links.join(ranks).flatMap {
                case (u, (uLinks:Iterable[Int], urank)) =>
                    uLinks.map(t =>
                        (t, urank / uLinks.size))

            }

            ranks = ranks.leftOuterJoin(contributions.reduceByKey((x, y) => x + y).mapValues(v => (0.15f / N) + 0.85f * v)).map(r => (r._1, r._2._2 match {
                case Some(value) => value
                case None => r._2._1
            }))
        }

        // sort in descending order by PageRank value
        ranks.sortBy(-_._2).collect().toList
    }

}
