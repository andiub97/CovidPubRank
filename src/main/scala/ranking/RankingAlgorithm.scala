package ranking

abstract class RankingAlgorithm {
    type T
    /**
     * Performs ranking of a graph's nodes via some policy
     *
     * @param edgesList list of graph's edges
     * @param N number of nodes in the graph
     * */
    def rank(edgesList: T, N: Int): List[(Int, Float)]
}
