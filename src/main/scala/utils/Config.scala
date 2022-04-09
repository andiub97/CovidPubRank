package utils

import com.typesafe.config._

object Config {
  val env = System.getenv("test")

  val conf = ConfigFactory.load()
  def apply() = conf.getConfig(env)
}
