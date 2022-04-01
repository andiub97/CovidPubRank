ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "CovidPubRank"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.1.2"
libraryDependencies += "org.scalatra.scalate" %% "scalate-core" % "1.9.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"