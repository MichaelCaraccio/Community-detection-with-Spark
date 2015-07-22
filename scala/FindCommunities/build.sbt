name := "FindCommunities"

version := "1.0"

scalaVersion := "2.10.5"

/*libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
    "org.apache.spark" %% "spark-graphx" % "1.3.0" % "provided",
    "org.apache.spark" %% "spark-mllib" % "1.3.0" % "provided")

libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.3.0"	

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.3.0-M1"*/

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.4.0" % "provided",
    "org.apache.spark" %% "spark-graphx" % "1.4.0" % "provided",
    "org.apache.spark" %% "spark-mllib" % "1.4.0" % "provided")

libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.4.0"	

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0-M1"

//libraryDependencies += "com.google.code.gson" % "gson" % "2.3"

//libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()

// http://stackoverflow.com/questions/28459333/how-to-build-an-uber-jar-fat-jar-using-sbt-within-intellij-idea
// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}

resolvers ++= Seq(
    // "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
    // "Spray Repository" at "http://repo.spray.cc/",
    // "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    // "Akka Repository" at "http://repo.akka.io/releases/",
    //  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
    //  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
    //  "Twitter Maven Repo" at "http://maven.twttr.com/",
    //  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
    //  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
    //  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/"
    //  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
    // Resolver.sonatypeRepo("public")
)