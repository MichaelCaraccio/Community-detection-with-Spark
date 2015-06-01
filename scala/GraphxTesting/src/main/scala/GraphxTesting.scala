import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import collection.JavaConversions._

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.util.matching.Regex

import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

// Useful links
// https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
// http://planetcassandra.org/getting-started-with-apache-spark-and-cassandra/
// https://bcomposes.wordpress.com/2013/02/09/using-twitter4j-with-scala-to-access-streaming-tweets/
// https://github.com/datastax/spark-cassandra-connector/blob/master/doc/5_saving.md

object GraphxTesting{
    
    def main(args: Array[String]) {
        
        // Display only warning and infos messages
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
        
        // Not displaying infos messages
        //Logger.getLogger("org").setLevel(Level.OFF)
        //Logger.getLogger("akka").setLevel(Level.OFF)
        
        // Spark configuration
        val sparkConf = new SparkConf(true)
        .setMaster("local[4]")
        .setAppName("GraphxTesting")
        .set("spark.cassandra.connection.host", "127.0.0.1") // Link to Cassandra

        val sc = new SparkContext(sparkConf)
        
        // Resulting graph
        val userGraph: Graph[(String, String), String]
        
        // Create an RDD for the vertices
        val users: RDD[(VertexId, (String))] = 
            sc.parallelize(Array((1L, ("Michael")), 
                                 (2L, ("David")),
                                 (3L, ("Sarah")),
                                 (4L, ("Jean")),
                                 (5L, ("Raphael")),
                                 (6L, ("Lucie")),
                                 (7L, ("Harold")),
                                 (8L, ("Pierre")),
                                 (9L, ("Christophe")),
                                 (10L, ("Zoe"))
                                ))
        
        // Create an RDD for edges
        val relationships: RDD[Edge[String]] =
            sc.parallelize(Array(Edge(1L, 2L, "1"),
                                 Edge(1L, 7L, "2"),
                                 Edge(1L, 6L, "3"),
                                 Edge(6L, 1L, "4"),
                                 Edge(7L, 8L, "5"),
                                 Edge(7L, 2L, "6"),
                                 Edge(2L, 10L, "7"),
                                 Edge(10L, 2L, "8"),
                                 Edge(10L, 3L, "9"),
                                 Edge(9L, 7L, "10"),
                                 Edge(9L, 6L, "11"),
                                 Edge(9L, 5L, "12"),
                                 Edge(5L, 9L, "13"),
                                 Edge(4L, 9L, "14"),
                                 Edge(8L, 7L, "15"),
                                 Edge(9L, 7L, "16"),
                                 Edge(6L, 1L, "17"),
                                 Edge(4L, 9L, "18"),
                                 Edge(7L, 9L, "19"),
                                 Edge(7L, 8L, "20"),
                                 Edge(7L, 8L, "21"),
                                 Edge(8L, 7L, "22"),
                                 Edge(8L, 7L, "23"),
                                 Edge(1L, 2L, "24"),
                                 Edge(7L, 2L, "25"),
                                 Edge(2L, 7L, "26"),
                                 Edge(2L, 7L, "27"),
                                 Edge(2L, 7L, "28"),
                                 Edge(6L, 1L, "29"),
                                 Edge(6L, 1L, "30"),
                                 Edge(1L, 7L, "31"), 
                                 Edge(1L, 7L, "32")
                                ))
        
        // Define a default user in case there are relationship with missing user
        val defaultUser = ("John Doe")
        
        // Build the initial Graph
        val graph = Graph(users, relationships, defaultUser)
        
        val facts: RDD[String] =
		graph.triplets.map(triplet =>
		triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
		facts.collect.foreach(println(_))
		
	}
        
    }
}