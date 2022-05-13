package ranking.ProvaPageRank

import org.apache.log4j.LogManager
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable


object PageRank {

  // Initialize k, number of iterations, and number of total vertices in synthetic graph
  val k = 1500;
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

  def computePageRank(sc: SparkContext) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    //val conf = new SparkConf().setAppName("Page Rank Spark").setMaster("local[*]")
    //val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. Comment out for local ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try {
    //      hdfs.delete(new org.apache.hadoop.fs.Path(args(0)), true)
    //    } catch {
    //      case _: Throwable => {}
    //    }
    // ================

    // Define partitioner
    val partitioner = new HashPartitioner(100)

    // Create synthetic graph
    val synthetic_graph = sc.parallelize(generateGraph(k)).partitionBy(partitioner)

    // Create an adjacency list RDD with (key, adj_list val) pairs
    val links  = synthetic_graph.groupByKey()
    var page_ranks = links.mapValues(node => (1.0 / (vertices)))

    // Compute sink node rank for dangling nodes and set its page rank score to to 0.
    // Add the sink node to the page ranks RDD
    val sink_node_rank = sc.parallelize(List((0, 0.0))).partitionBy(partitioner)
    page_ranks = page_ranks.union(sink_node_rank)

    //Iterate through the graph
    for (itr_count <- 1 to iterations){

      // Compute the incoming contributions for all nodes that receive in-links and aggregate the PR value per node
      val inc_contribs = links.join(page_ranks)
        .flatMap{case(pageID, (links, rank)) => links.map(page => (page, rank/links.size))}
        .reduceByKey(_+_)

      // Compute and collect the sink nodes mass from all incoming contributions into sink node
      val sink_node_mass = inc_contribs.lookup(0)(0)

      // Distribute the mass evenly among all nodes
      val mass_distribution = sink_node_mass / (vertices)

      // Compute final contributions, filtering out the sink node
      val full_contributions = page_ranks
        .leftOuterJoin(inc_contribs)
        .filter(_._1 != 0)
        .mapValues{case(x,y) => if (y.isDefined) y.get else 0.0 }

      // Update the page rank values for current iteration
      page_ranks = full_contributions.mapValues(v => 0.15/(vertices) + 0.85*(v + mass_distribution)).persist()
    }

    page_ranks.sortBy(_._1, true).saveAsTextFile("src/main/scala/output")
    logger.info(page_ranks.toDebugString)

  }
}