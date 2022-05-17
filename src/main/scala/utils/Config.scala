package utils

import com.typesafe.config._

object Config {
  val env: String = System.getenv("test")

  val conf: Config = ConfigFactory.load()
  def apply(): Config = conf.getConfig(env)
}
