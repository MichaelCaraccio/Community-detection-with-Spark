name := "ScalaTwitterStreaming"

version := "1.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"              % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-streaming"         % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.2.0")

libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "3.0.3"