name := "FinalProject"

version := "1.0"

scalaVersion := "2.10.5"

// SPARK 1.2.0
/*libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"              % "1.2.0" % "provided",
  "org.apache.spark" %% "spark-graphx"            % "1.2.0" % "provided")

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.1"*/

// SPARK 1.3.0
/*libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"              % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-graphx"            % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-mllib"             % "1.3.0" % "provided")*/

// SPARK 1.4.0
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core"              % "1.4.0" % "provided",
    "org.apache.spark" %% "spark-graphx"            % "1.4.0" % "provided",
    "org.apache.spark" %% "spark-streaming"         % "1.4.0" % "provided",
    "org.apache.spark" %% "spark-mllib"             % "1.4.0" % "provided",
    "org.apache.spark" %% "spark-streaming-twitter" % "1.2.1")

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0-M1"
//libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.1.2"
libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()