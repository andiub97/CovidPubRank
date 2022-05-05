import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import ranking._
import ranking.algorithmTraits.{AlgorithmInterface, LibraryAlgorithms, ListAlgorithms, NotLibraryAlgorithms}
import utils.{FileUtility, SparkContextSingleton, VisualizationUtils}

object Main {


    def performRanking(graphFilePath: String, edgesList: RDD[(Int, Int)], N: Int, algorithm: AlgorithmInterface): (String, (RDD[(Int, Float)], Double)) = {

        val sc = SparkContextSingleton.getContext
        algorithm.setContext(sc)
        algorithm match {

            case r @ (_: DistributedPageRank | _: DistributedPageRankOptimized ) =>
                val start_time = System.nanoTime
                //val distEdgesList = sc.parallelize(edgesList)
                var ranking: RDD[(Int,Float)] = null
                ranking = r.asInstanceOf[NotLibraryAlgorithms].rank(edgesList: RDD[(Int,Int)],N)

                val duration = (System.nanoTime - start_time) / 1e9d
                "ranking.DistributedPageRank" -> (ranking, duration)

            case r : PageRank =>
                val start_time = System.nanoTime

                val ranking = r.asInstanceOf[ListAlgorithms].rank(edgesList.collect().toList: List[(Int,Int)],N)
                val duration = (System.nanoTime - start_time) / 1e9d
                r.getClass.getName -> (ranking, duration)

            case r @ (_ : PageRankLibrary | _: ParallelPageRankLibrary ) =>
                // get nodes from file
                val nodes = FileUtility.loadNodesFromFile(graphFilePath)
                val distNodes = nodes.map(i => (i._1.toLong, (i._1,i._2)))

                //graphs' edges
                val edgeList = FileUtility.loadEdgesFromFile(graphFilePath).map(x => Edge(x._1, x._2,""))

                val graph = Graph(distNodes, edgeList)

                val start_time = System.nanoTime
                val ranking = r.asInstanceOf[LibraryAlgorithms].rank(graph, N)
                val duration = (System.nanoTime - start_time) / 1e9d
                r.getClass.getName -> (ranking, duration)

        }
    }

    def main(args: Array[String]): Unit = {
        // Parse program arguments
        print("ciao")
        val par = if (args.length > 0) args(0) else "local"
        val algorithmName = if (args.length > 1) args(1) else "allAlgorithms"
        val graphFilePath = if (args.length > 2) args(2) else "data/dataset_9648.txt"
        val outputFilePath = if (args.length > 3) args(3) else "src/main/scala/output"

        val sparkSession = SparkContextSingleton.sparkContext(par)

        val distributedAlgorithm: AlgorithmInterface = FileUtility.chooseDistributedPageRank(graphFilePath: String)
        // Chart size
        val topK: Int = 3
        // Pick the ranking algorithm
        val r : List[AlgorithmInterface] =
            algorithmName match {
                case "DistributedPageRank" => List(distributedAlgorithm)
                case "PageRank" => List( new PageRank())
                case "PageRankLibrary" => List(new PageRankLibrary())
                case "ParallelPageRankLibrary" => List(new ParallelPageRankLibrary())
                case "NotDistributedAlgorithms" => List(new PageRank(), new PageRankLibrary())
                case "DistributedAlgorithms" => List(distributedAlgorithm, new ParallelPageRankLibrary())
                case "allAlgorithms" => List(distributedAlgorithm, new ParallelPageRankLibrary(), new PageRankLibrary())
            }

        // Report algorithm
        println("Using algorithm "+algorithmName)
        println("Loading graph from "+graphFilePath)

        val edgesList = FileUtility.loadEdgesFromFile(graphFilePath)
        val nodes = FileUtility.loadNodesFromFile(graphFilePath)
        val N: Int = nodes.count().toInt
        // Display graph data
        println("Loaded " + N + " nodes.")
        println("Loaded " + edgesList.count() + " edges.")
        // Perform ranking
        var ranking: Map[String, (RDD[(Int, Float)], Double)] = null

        ranking = r.map(algorithm => performRanking(graphFilePath, edgesList, N, algorithm)).take(topK).toMap
        // Print all the results
        ranking.map(r => (println(r._1), println(r._2._2), VisualizationUtils.printTopK(r._2._1, nodes, topK)))
        // Get execution time for each ranking algorithm
        val exec_times = ranking.map(r => (r._1, r._2._2 ))
        // Export results to txt file

        FileUtility.exportAlgorithmsResults(outputFilePath, exec_times, args(0), args(2))

    }


}
