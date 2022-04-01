import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import ranking._
import ranking.algorithmTraits.{AlgorithmInterface, LibraryAlgorithms, ListAlgorithms}
import utils.{FileUtility, SparkContextSingleton, VisualizationUtils}

object Main {


    def performRanking(graphFilePath: String, edgesList: List[(Int, Int)], N: Int, algorithm: AlgorithmInterface): (String, (List[(Int, Float)], Double)) = {
        val sc = SparkContextSingleton.getContext
        algorithm.setContext(sc)
        algorithm match {

            case r @ (_: DistributedPageRank ) =>
                val start_time = System.nanoTime
                val distEdgesList = sc.parallelize(edgesList)
                var ranking: List[(Int,Float)] = null

                if ((graphFilePath == "data/citations_1.txt") || (graphFilePath == "gs://citations_bucket/citations_1.txt")){
                    ranking = r.asInstanceOf[DistributedPageRankOptimized].rank(distEdgesList: RDD[(Int,Int)],N)
                }else{
                    ranking = r.rank(distEdgesList: RDD[(Int,Int)],N)
                }

                val duration = (System.nanoTime - start_time) / 1e9d
                r.getClass.getName -> (ranking, duration)



            case r : PageRank =>
                val start_time = System.nanoTime
                val ranking = r.asInstanceOf[ListAlgorithms].rank(edgesList: List[(Int,Int)],N)
                val duration = (System.nanoTime - start_time) / 1e9d
                r.getClass.getName -> (ranking, duration)

            case r @ (_ : PageRankLibrary | _: ParallelPageRankLibrary ) =>
                // get nodes from file
                val nodes = FileUtility.loadNodesFromFile(graphFilePath).toSeq
                val vertexMap = nodes.indices.map(i => nodes(i) -> nodes(i)._1.toLong).toMap
                val distNodes = sc.parallelize(vertexMap.toSeq.map(_.swap))

                //graphs' edges
                val edgeList = sc.parallelize(FileUtility.loadGraphFromFile(graphFilePath)).map(x => Edge(x._1, x._2,""))

                val graph = Graph(distNodes,edgeList)

                val start_time = System.nanoTime
                val ranking = r.asInstanceOf[LibraryAlgorithms].rank(graph, N)
                val duration = (System.nanoTime - start_time) / 1e9d
                r.getClass.getName -> (ranking, duration)

        }
    }

    def main(args: Array[String]): Unit = {
        // Parse program arguments
        val algorithmName = if (args.length > 0) args(0) else "allAlgorithms"
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
                case "NotDistributedAlgorithms" => List(new PageRank(), new PageRankLibrary())
                case "DistributedAlgorithms" => List(new DistributedPageRank(), new ParallelPageRankLibrary())
                case "allAlgorithms" => List(new DistributedPageRank(), new ParallelPageRankLibrary(), new PageRank(), new PageRankLibrary())
            }

        // Report algorithm
        println("Using algorithm "+algorithmName)
        println("Loading graph from "+graphFilePath)


            val edgesList = FileUtility.loadGraphFromFile(graphFilePath)
            val nodes = FileUtility.loadNodesFromFile(graphFilePath)
            val N: Int = nodes.size
            // Display graph data
            println("Loaded " + N + " nodes.")
            println("Loaded " + edgesList.size + " edges.")
            // Perform ranking
            var ranking: Map[String, (List[(Int, Float)], Double)] = null

            ranking = r.map(algorithm => performRanking(graphFilePath, edgesList, N, algorithm)).toMap
            // Print all the results
            ranking.map(r => (println(r._1), println(r._2._2), VisualizationUtils.printTopK(r._2._1, nodes, k = topK)))
            // Get execution time for each ranking algorithm
            val exec_times = ranking.map(r => (r._1, (r._2._2, graphFilePath )))
            // Export results to txt file

            FileUtility.exportAlgorithmsResults(outputFilePath, exec_times)

    }


}
