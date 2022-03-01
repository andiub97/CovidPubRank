import org.apache.spark.graphx.{Edge, Graph}
import ranking.{DistributedPageRank, PageRank, PageRankLibrary, RankingAlgorithm}
import utils.{FileUtility, SparkContextSingleton, VisualizationUtils}


object Main {

    def performRanking(graphFilePath: String, edgesList: List[(Int, Int)], N: Int, algorithm: RankingAlgorithm): List[(Int, Float)] = {
        algorithm match {
            case r: PageRank => r.rank(edgesList, N)
            case r: DistributedPageRank =>
                val sc = SparkContextSingleton.getContext
                val distEdgesList = sc.parallelize(edgesList)
                r.setContext(sc)
                r.rank(distEdgesList, N)
            case r: PageRankLibrary =>
                val sc = SparkContextSingleton.getContext
                r.setContext(sc)

                val nodes = FileUtility.loadNodesFromFile(graphFilePath).toSeq
                val vertexMap = (0 until nodes.size).map(i => nodes(i) -> nodes(i)._1.toLong).toMap
                vertexMap.foreach(m => (println(m._1), println(m._2)))
                val edgeList = FileUtility.loadGraphFromFile(graphFilePath).map(x => Edge((x._1),(x._2),"")).toSeq

                val distEdgesList = sc.parallelize(edgeList.toSeq)
                val distNodes = sc.parallelize(vertexMap.toSeq.map(_.swap))

                val graph = Graph(distNodes,distEdgesList)
                r.rank(graph, N)

        }
    }

    def main(args: Array[String]): Unit = {
        // Parse program arguments
        val graphFilePath = "data/citations_500.txt"
        val algorithmName = if (args.length > 1) args(1) else "PageRankLibrary"
        // PageRank tolerance
        val prTolerance: Float = 0.000000001f
        // Output parameters
        val topK: Int = 3
        //val outputFilename: String = "result_%s.html".format(algorithmName)
        // Pick the ranking algorithm
        val r : RankingAlgorithm =
            algorithmName match {
                case "DistributedPageRank" => new DistributedPageRank()
                case "PageRank" => new PageRank(tolerance = prTolerance)
                case "PageRankLibrary" => new PageRankLibrary()
                }

        // Report algorithm
        println("Using algorithm "+algorithmName);
        // Load data
        println("Loading graph from "+graphFilePath);
        val edgesList = FileUtility.loadGraphFromFile(graphFilePath)
        val nodes = FileUtility.loadNodesFromFile(graphFilePath)
        val N: Int = nodes.size
        // Display graph data
        println("Loaded "+N+" nodes.")
        println("Loaded "+edgesList.size+" edges.")
        // Perform ranking
        val ranking = performRanking(graphFilePath, edgesList, N, r)
        // Print all the results
        VisualizationUtils.printTopK(ranking, nodes, k = topK)

    }


}
