package utils

import org.apache.spark.{SparkConf, SparkContext}

object SparkContextSingleton {
    private var context: SparkContext = _

    private def initializeContext(): Unit = {
        // create instance
        val conf = new SparkConf()
          .set("spark.driver.memory", "6g")
          .set("spark.executor.memory", "14g")
          .set("spark.driver.maxResultSize", "10g")
          .set("spark.sql.shuffle.partition", "4")
          .set("spark.default.parallelism", "4")
          .set("spark.yarn.executor.memoryOverhead", "2048")
          .setAppName("CovidPubRank")
          .setMaster("local[*]")

        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        // configure
        /*val hadoopConfig = sc.hadoopConfiguration
        hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
        hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)*/
        // get the reference
        this.context = sc
    }

    def getContext: SparkContext = {
        if (context == null) initializeContext()
        this.context
    }
}
