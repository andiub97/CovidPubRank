package ranking.algorithmTraits

import org.apache.spark.SparkContext
import utils.SparkContextSingleton

trait AlgorithmInterface {
  type T
  var context: SparkContext = SparkContextSingleton.getContext

  def setContext(sc: SparkContext): Unit = {
    this.context = sc
  }
  def rank(edgesList: T, N: Int): List[(Int, Float)]
}
