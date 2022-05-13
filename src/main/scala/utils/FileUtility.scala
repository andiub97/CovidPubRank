package utils

import org.apache.spark.{RangePartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import ranking.algorithmTraits.AlgorithmInterface
import ranking.{DistributedPageRank, DistributedPageRankOptimized}

import java.io.{FileWriter, PrintWriter}


object FileUtility {

    def chooseDistributedPageRank(str: String): AlgorithmInterface = {
        if ((str == "data/citations_1.txt") || (str == "gs://dataset_citation/citations_1.txt")){
            new DistributedPageRankOptimized()
        } else{
            new DistributedPageRank()
        }
    }


    def exportAlgorithmsResults(path: String, exec_times: Map[String, Double], numWorker: String, dataset: String, sparkContext:SparkContext): Unit = {

        if(numWorker == "local"){
            val pw = new PrintWriter(new FileWriter(path))
            exec_times.foreach(e => (pw.print(e._1 + ","), pw.print(e._2 + ","), pw.println(dataset)))
            pw.close()

        }else {
            if(numWorker != "local"){
                sparkContext.parallelize(exec_times.map(t => (t._1, t._2, numWorker)).toSeq).coalesce(1,true).saveAsTextFile(path)

            }else{
                sparkContext.parallelize(exec_times.toSeq).coalesce(1,true).saveAsTextFile(path)
            }
        }


    }

    /**
     * Loads a graph's list of edges from a given file path.
     * */
    def loadEdgesFromFile(path: String, sparkContext: SparkContext): RDD[(Int, Int)] = {

        val graphFile = sparkContext.textFile(path)
        val edges: RDD[(Int, Int)] = graphFile
          .filter(line => line.startsWith("e"))
          .map(line => line.split("\\s"))
          .map(tokens => (tokens(1).toInt, tokens(2).toInt))

        edges.partitionBy(new RangePartitioner(SparkContextSingleton.DEFAULT_PARALLELISM, edges)).persist(StorageLevel.MEMORY_AND_DISK)
    }

    /**
     * Loads node labels from a given file path.
     * */
    def loadNodesFromFile(path: String, sparkContext: SparkContext): RDD[(Int, String)] = {

        val graphFile = sparkContext.textFile(path)
        val nodes: RDD[(Int, String)] = graphFile
          .filter(line => line.startsWith("n"))
          .map(line => line.split("\\s").splitAt(2))
          .map(tokens => (tokens._1(1).toInt, tokens._2.mkString(" ")))

        nodes.partitionBy(new RangePartitioner(SparkContextSingleton.DEFAULT_PARALLELISM, nodes)).persist(StorageLevel.MEMORY_AND_DISK)

        nodes
    }

}
