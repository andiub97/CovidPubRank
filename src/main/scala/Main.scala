import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import ranking._
import ranking.algorithmTraits.{AlgorithmInterface, LibraryAlgorithms, NotLibraryAlgorithms}
import utils.{FileUtility, SparkContextSingleton, VisualizationUtils}

object Main {


    def performRanking(graphFilePath: String, edgesList: RDD[(Int, Int)], N: Int, algorithm: AlgorithmInterface): (String, (List[(Int, Float)], Double)) = {
        val sc = SparkContextSingleton.getContext
        algorithm.setContext(sc)
        algorithm match {
            case r @ ( _: PageRank | _: DistributedPageRank ) =>

                val start_time = System.nanoTime
                val ranking = r.asInstanceOf[NotLibraryAlgorithms].rank(edgesList: RDD[(Int,Int)],N)
                val duration = (System.nanoTime - start_time) / 1e9d
                r.getClass.getName -> (ranking, duration)

            case r @ (_ : PageRankLibrary | _ : ParallelPageRankLibrary ) =>
                // get nodes from file
                val nodes = FileUtility.loadNodesFromFile(graphFilePath).toSeq
                val vertexMap = nodes.indices.map(i => nodes(i) -> nodes(i)._1.toLong).toMap
                val distNodes = sc.parallelize(vertexMap.toSeq.map(_.swap))

                //graphs' edges
                val edgeList = FileUtility.loadGraphFromFile(graphFilePath).map(x => Edge(x._1, x._2,""))

                val graph = Graph(distNodes,edgeList)

                val start_time = System.nanoTime
                val ranking = r.asInstanceOf[LibraryAlgorithms].rank(graph, N)
                val duration = (System.nanoTime - start_time) / 1e9d
                r.getClass.getName -> (ranking, duration)

        }
    }

    def main(args: Array[String]): Unit = {
        // Parse program arguments
        val algorithmName = if (args.length > 0) args(0) else "DistributedPageRank"
        val graphFilePath = if (args.length > 1) args(1) else "data/citations_500.txt"
        val outputFilePath = if (args.length > 2) args(2) else "src/main/scala/output"
        // Chart size
        val topK: Int = 3
        // Pick the ranking algorithm
        val r : List[AlgorithmInterface] =
            algorithmName match {
                case "DistributedPageRank" => List( new DistributedPageRank())
                case "PageRank" => List( new PageRank())
                case "PageRankLibrary" => List(new PageRankLibrary())
                case "ParallelPageRankLibrary" => List(new ParallelPageRankLibrary())
                case "AllRankingAlgorithms" => List( new DistributedPageRank(), new PageRank(), new PageRankLibrary(), new ParallelPageRankLibrary())
            }

        // Report algorithm
        println("Using algorithm "+algorithmName)
        println("Loading graph from "+graphFilePath)
        val edgesList = FileUtility.loadGraphFromFile(graphFilePath)
        val nodes = FileUtility.loadNodesFromFile(graphFilePath)
        val N: Int = nodes.size
        // Display graph data
        println("Loaded "+N+" nodes.")
        println("Loaded "+edgesList.count() +" edges.")
        // Perform ranking
        var ranking: Map[String, (List[(Int, Float)], Double)] = null
        for (elem <- r) {
            println(elem.getClass.getName)

        }
        ranking = r.map(a => performRanking(graphFilePath, edgesList, N, a)).toMap
        // Print all the results
        ranking.map( r => (println(r._1),println(r._2._2), VisualizationUtils.printTopK(r._2._1, nodes, k = topK)))
        // Get execution time for each ranking algorithm
        val exec_times = ranking.map(r => (r._1, r._2._2))
        // Export results to txt file
        FileUtility.exportToTxt(outputFilePath, exec_times)
    }


}
