package ranking.algorithmTraits

import org.apache.spark.rdd.RDD

trait ListAlgorithms extends AlgorithmInterface {

  type T = List[(Int, Int)]
  override def rank(edgesList: T, N: Int): RDD[(Int, Float)]

}
