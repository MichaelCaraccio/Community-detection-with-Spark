name := "ScalaTwitterStreaming"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"              % "1.2.1" % "provided",
  "org.apache.spark" %% "spark-streaming"         % "1.2.1" % "provided",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.2.1")

//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.1"

//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.2.1"

//libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.2.1"

//libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.2.1"    

//libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.2.1"

libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "3.0.3"

//mainClass in (Compile, packageBin) := Some("ScalaTwitterStreaming.main")

//mainClass in (Compile, run) := Some("ScalaTwitterStreaming.main")
