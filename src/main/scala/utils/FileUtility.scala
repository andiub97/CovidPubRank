package utils

import ranking.{DistributedPageRank, DistributedPageRankOptimized}
import ranking.algorithmTraits.AlgorithmInterface

import java.io.{FileWriter, PrintWriter}


object FileUtility {

    def chooseDistributedPageRank(str: String): AlgorithmInterface = {
        if ((str == "data/citations_1.txt") || (str == "gs://citations_bucket/citations_1.txt")){
            new DistributedPageRankOptimized()
        } else{
            new DistributedPageRank()
        }
    }


    def exportAlgorithmsResults(path: String, exec_times: Map[String, (Double, String)]): Unit = {
        //SparkContextSingleton.getContext.parallelize(exec_times.toSeq).coalesce(1,true).saveAsTextFile(path)
        val pw = new PrintWriter(new FileWriter(path))
        pw.println("algorithmName,exec_time,citation")
        exec_times.foreach(e => (pw.print(e._1 + ","), pw.print(e._2._1 + ","), pw.println(e._2._2)))
        pw.close()
    }




    /**
     * Loads a graph's list of edges from a given file path.
     * */
    def loadGraphFromFile(path: String): List[(Int, Int)] = {
        val graphFile = SparkContextSingleton.getContext.textFile(path)
        val edgesList: List[(Int, Int)] = graphFile
          .filter(line => line.startsWith("e"))
          .map(line => line.split("\\s"))
          .map(tokens => (tokens(1).toInt, tokens(2).toInt)).collect().toList
          edgesList
    }

    /**
     * Loads node labels from a given file path.
     * */
    def loadNodesFromFile(path: String): Map[Int, String] = {
        val graphFile = SparkContextSingleton.getContext.textFile(path)
        val nodes: Map[Int, String] = graphFile
          .filter(line => line.startsWith("n"))
          .map(line => line.split("\\s").splitAt(2))
          .map(tokens => (tokens._1(1).toInt, tokens._2.mkString(" ")))
          .collect.toMap[Int, String]
        nodes
    }

}
