package utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

object SparkContextSingleton {
    private var context: SparkContext = _

    val conf = ConfigFactory.load()
    val local_master: String = conf.getString("CovidPubRank.LOCAL_MASTER")
    val cluster_master: String = conf.getString("CovidPubRank.CLUSTER_MASTER")
    val local_par : String = conf.getString("CovidPubRank.LOCAL_PARALLELISM")
    val work1_par : String = conf.getString("CovidPubRank.1WORKER_PARALLELISM")
    val work2_par : String = conf.getString("CovidPubRank.2WORKER_PARALLELISM")
    val work3_par : String = conf.getString("CovidPubRank.3WORKER_PARALLELISM")
    val work4_par : String = conf.getString("CovidPubRank.4WORKER_PARALLELISM")


    def _sparkSession(par: String, master: String): SparkSession = {
        var builder = SparkSession.builder.appName("CovidPubRank")
                            .config("spark.driver.memory", "6g")
                            .config("spark.executor.memory", "14g")
                            .config("spark.driver.maxResultSize", "10g")
                            .config("spark.default.parallelism", par)
                            .config("spark.yarn.executor.memoryOverhead", "2048")
                            .master(master)

        builder.getOrCreate()
    }

    def sparkContext(conf: String): SparkContext = {

        val session = conf match {

            case "local" => _sparkSession(local_par, local_master)
            case "single-node" => _sparkSession(work1_par, cluster_master)
            case "2-worker" => _sparkSession(work2_par, cluster_master)
            case "3-worker" => _sparkSession(work3_par, cluster_master)
            case "4-worker" => _sparkSession(work4_par, cluster_master)
            case default => _sparkSession(local_par,local_master)
        }

        session.sparkContext.setLogLevel("WARN")

        this.context = session.sparkContext
        this.context
    }

    def getContext: SparkContext = {

        this.context
    }


    /*private def initializeContext(): Unit = {
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

    def getContext(master: String): SparkContext = {

        if (context == null) initializeContext()
        this.context
    }*/
}
