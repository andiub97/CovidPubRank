package utils

import org.apache.spark.sql.SparkSession

object SparkContextSingleton {
    var DEFAULT_PARALLELISM = 1 // number of partitions

    private def _sparkSession(master: String): SparkSession = {
        var builder = SparkSession.builder.appName("CovidPubRank")

        if (master != "default") {
            builder = builder.master(master)
        }

        builder.getOrCreate()
    }

    def sparkSession(master: String, par: Int): SparkSession = {
        val session = _sparkSession(master)

        DEFAULT_PARALLELISM = par
        session.sparkContext.setLogLevel("WARN")

        session
    }
}
