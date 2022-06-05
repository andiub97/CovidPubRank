package ranking.algorithmTraits

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait NotLibraryAlgorithms extends AlgorithmInterface {

  override type T = RDD[(Int, Int)]
  def rank(edgesList: T, N: Int, sparkContext: SparkContext): RDD[(Int, Float)]
  override def toString: String =
    if(this.getClass.getName == "ranking.DistributedPageRankOptimized")
      "ranking.DistributedPageRank"
    else
      this.getClass.getName

}
