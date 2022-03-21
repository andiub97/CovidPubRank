package utils

import org.apache.spark.rdd.RDD

import java.io.{FileWriter, PrintWriter}


object FileUtility {

    /**
     * Loads a graph's list of edges from a given file path.
     * */
    def loadGraphFromFile(path: String): RDD[(Int, Int)] = {
        val graphFile = SparkContextSingleton.getContext.textFile(path)
        val edgesList: RDD[(Int, Int)] = graphFile
          .filter(line => line.startsWith("e"))
          .map(line => line.split("\\s"))
          .map(tokens => (tokens(1).toInt, tokens(2).toInt))
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

    def exportToTxt(path: String, exec_times: Map[String, Double]): Unit = {
        //SparkContextSingleton.getContext.parallelize(exec_times.toSeq).saveAsTextFile(path)
        val pw = new PrintWriter(new FileWriter(path))
        pw.println("algorithmName,exec_time")
        exec_times.foreach(e => (pw.print(e._1 + ","), pw.println(e._2)))
        pw.close()

    }

}
