import org.apache.spark.{SparkConf, SparkContext}
import ranking.{PageRank, RankingAlgorithm}
import utils.{FileUtils, SparkContextSingleton, VisualizationUtils}


object Main {

    def performRanking(edgesList: List[(Int, Int)], N: Int, algorithm: RankingAlgorithm): List[(Int, Float)] = {
        algorithm match {
            //case r: InDegreeRank => r.rank(edgesList, N)
            case r: PageRank =>
                val sc = SparkContextSingleton.getContext
                val distEdgesList = sc.parallelize(edgesList)
                r.setContext(sc)
                r.rank(distEdgesList, N)

        }
    }

    def main(args: Array[String]): Unit = {
        // Parse program arguments
        val graphFilePath = if (args.length > 0) args(0) else "data/citations_500.txt"
        val algorithmName = if (args.length > 1) args(1) else "PageRank"
        // PageRank tolerance
        val prTolerance: Float = 0.000000001f
        // Output parameters
        val topK: Int = 3
        //val outputFilename: String = "result_%s.html".format(algorithmName)
        // Pick the ranking algorithm
        val r : RankingAlgorithm = algorithmName match {
            //case "InDegreeRank" => new InDegreeRank
            case "PageRank" => new PageRank(tolerance = prTolerance)
        }
        // Report algorithm
        println("Using algorithm "+algorithmName);
        // Load data
        println("Loading graph from "+graphFilePath);
        val edgesList = FileUtils.loadGraphFromFile(graphFilePath)
        val nodes = FileUtils.loadNodesFromFile(graphFilePath)
        val N: Int = nodes.size
        // Display graph data
        println("Loaded "+N+" nodes.")
        println("Loaded "+edgesList.size+" edges.")
        // Perform ranking
        val ranking = performRanking(edgesList, N, r)
        // Print all the results
        VisualizationUtils.printTopK(ranking, nodes, k = topK)
        // Output results to a html page
        //VisualizationUtils.outputHtmlPage(outputFilename, graphFilePath, ranking, nodes, k = topK)
        //println("Saved results in "+outputFilename);
    }
}
