package utils

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import ranking.algorithmTraits.AlgorithmInterface
import ranking.{DistributedPageRank, DistributedPageRankOptimized}

import java.io.{FileWriter, PrintWriter}
import scala.io.Source


object FileUtility {

    val parallelism = SparkContextSingleton.getContext.getConf.get("spark.default.parallelism").toInt

    def chooseDistributedPageRank(str: String): AlgorithmInterface = {
        if ((str == "data/citations_1.txt") || (str == "gs://citations_bucket/data/dataset_1015682.txt")){
            new DistributedPageRankOptimized()
        } else{
            new DistributedPageRank()
        }
    }


    def exportAlgorithmsResults(path: String, exec_times: Map[String, Double], numWorker: String, dataset: String): Unit = {

        val pw = new PrintWriter(new FileWriter(path))
        exec_times.foreach(e => (pw.print(e._1 + ","), pw.print(e._2 + ","), pw.println(dataset)))
        pw.close()

        /*if(numWorker != "local"){
            SparkContextSingleton.getContext.parallelize(exec_times.map(t => (t._1, t._2, numWorker)).toSeq).coalesce(1,true).saveAsTextFile(path)

        }else{
            SparkContextSingleton.getContext.parallelize(exec_times.toSeq).coalesce(1,true).saveAsTextFile(path)
        }*/

    }




    /**
     * Loads a graph's list of edges from a given file path.
     * */
    def loadEdgesFromFile(path: String): RDD[(Int, Int)] = {

        //val graphFile = SparkContextSingleton.getContext.textFile(path)
        val graphFile = Source.fromFile(path).getLines
        val edgesList: RDD[(Int, Int)] = SparkContextSingleton.getContext.parallelize(graphFile
          .filter(line => line.startsWith("e"))
          .map(line => line.split("\\s"))
          .map(tokens => (tokens(1).toInt, tokens(2).toInt)).toSeq,parallelism)
          edgesList
    }

    /**
     * Loads node labels from a given file path.
     * */
    def loadNodesFromFile(path: String): RDD[(Int, String)] = {
        val graphFile = Source.fromFile(path).getLines
        //val graphFile = SparkContextSingleton.getContext.textFile(path)
        val nodes: RDD[(Int, String)] = SparkContextSingleton.getContext.parallelize(graphFile
          .filter(line => line.startsWith("n"))
          .map(line => line.split("\\s").splitAt(2))
          .map(tokens => (tokens._1(1).toInt, tokens._2.mkString(" "))).toSeq,parallelism)
        nodes
    }

}
