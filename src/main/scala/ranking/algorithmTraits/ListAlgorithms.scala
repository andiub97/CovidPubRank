package ranking.algorithmTraits

trait ListAlgorithms extends AlgorithmInterface {

  type T = List[(Int, Int)]
  override def rank(edgesList: T, N: Int): List[(Int, Float)]

}
