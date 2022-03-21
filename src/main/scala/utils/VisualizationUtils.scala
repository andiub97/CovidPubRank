package utils


object VisualizationUtils {

    /**
     * Prints the top K articles according to the rank.
     *
     * @param rank    : ranking of articles.
     * @param nodes  : articles labels and titles.
     * @param k          : number of articles to print.
     * */
    def printTopK(rank: List[(Int, Float)], nodes: Map[Int, String], k: Int = 10): Unit = {
        val limit = k.min(rank.length)

        for (i <- 0 until limit) {
            println(nodes(rank(i)._1), rank(i)._1, rank(i)._2)

        }
    }

}
