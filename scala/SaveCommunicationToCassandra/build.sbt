name := "SaveCommunicationToCassandra"

version := "1.0"

scalaVersion := "2.10.4"

//resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"              % "1.4.0" % "provided",
  "org.apache.spark" %% "spark-streaming"         % "1.4.0" % "provided",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.2.1")

libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "3.0.3"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0-M1"

//libraryDependencies += "spark.jobserver" %% "job-server-api" % "0.5.1"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"