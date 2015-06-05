name := "GraphxTesting"

version := "1.0"

scalaVersion := "2.10.5"

// SPARK 1.2.0
/*libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"              % "1.2.0" % "provided",
  "org.apache.spark" %% "spark-graphx"            % "1.2.0" % "provided")

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.1"*/

// SPARK 1.3.0
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"              % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-graphx"            % "1.3.0" % "provided")
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.3.0-M1"
