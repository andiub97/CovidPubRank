package ranking.ProvaPageRank
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.Not

import scala.collection.mutable
import scala.util.Random
import org.apache.spark.sql.SparkSession


// Scala Program to only create a synthetic graph with k=1000 to use in Java - MapReduce program

object GraphPageRank {

  // Initiliaze k, number of iterations, and number of total vertices in synthetic graph
  val k = 10;
  val iterations = 10
  val vertices: Int = k * k

  // Function to create synthetic graph
  def generateGraph(k: Int): Seq[(Int, Int)] = {
    val edges: mutable.Set[(Int, Int)] = mutable.Set.empty
    var node = 1;

    while (node <= k * k) {
      if (node % k == 0) {
        edges.+=((node, 0))
      }
      else {
        edges.+=((node, node + 1))
      }
      node += 1
    }
    edges.toSeq
  }

  def computeGraph() {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val conf = new SparkConf().setAppName("Page Rank Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
   /* val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(args(0)), true)
    } catch {
      case _: Throwable => {}
    }*/
    // ================

    // Define partitioner
    val partitioner = new HashPartitioner(1)

    // Create synthetic graph
    val synthetic_graph = sc.parallelize(generateGraph(k)).partitionBy(partitioner)

    // Create an adjacency list RDD with (key, adj_list val) pairs
    val links  = synthetic_graph.groupByKey().map{case(k,v) => (k, v.toList(0))}

    var page_ranks = links.mapValues(node => (1.0 / (vertices)))

    // Compute sink node rank for dangling nodes and set its page rank score to to 0.
    // Add the sink node to the page ranks RDD
    val sink_node_rank = sc.parallelize(List((0, 0.0))).partitionBy(partitioner)
    page_ranks = page_ranks.union(sink_node_rank)

    // Join ranks and links to create vertex objects to use in Java MapReduce program
    val vertex_objects = page_ranks.join(links)
    vertex_objects
    //vertex_objects.sortBy(_._1, true,numPartitions = 1).map{case(x,y) => x + "," + y._1 + "," + y._2}//.saveAsTextFile(args(0))

    //vertex_objects.mapPartitionsWithIndex( (index: Int, it: Iterator[(Int,(Double,Int))]) =>it.toList.map(x => if (index == 0) {println(x)}).iterator).collect
  }
}