package utils

import org.apache.spark.rdd.RDD


object VisualizationUtils {

    /**
     * Prints the top K articles according to the rank.
     *
     * @param rank    : ranking of articles.
     * @param nodes  : articles labels and titles.
     * @param topK          : number of articles to print.
     * */
    def printTopK(rank: Array[(Int, Float)], nodes: RDD[(Int, String)], topK: Int): Unit = {
        val topKNodes = nodes.collect.toMap


        for (i <- 0 until topK) {

            println(topKNodes(i), rank(i)._2)
        }
    }

}
