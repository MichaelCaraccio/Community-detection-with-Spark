import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

//import org.apache.spark.streaming.twitter
//import org.apache.spark.streaming.twitter._
//import org.apache.spark.streaming.twitter.TwitterUtils

import org.apache.spark.SparkConf

//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.Seconds
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.StreamingContext._

import collection.JavaConversions._

import org.apache.log4j.Logger
import org.apache.log4j.Level

// Enable Cassandra-specific functions on the StreamingContext, DStream and RDD:
import com.datastax.spark.connector._ 
import com.datastax.spark.connector.streaming._

import scala.util.matching.Regex
import org.apache.spark.rdd.RDD


// Useful links
// https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
// http://planetcassandra.org/getting-started-with-apache-spark-and-cassandra/
// https://bcomposes.wordpress.com/2013/02/09/using-twitter4j-with-scala-to-access-streaming-tweets/
// https://github.com/datastax/spark-cassandra-connector/blob/master/doc/5_saving.md

object RDDFromCassandra {
    def main(args: Array[String]) {

        // Display only warning messages
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)

        val filters = args
        
        // Spark configuration
        val sparkConf = new SparkConf(true)
        .setMaster("local[4]")
        .setAppName("RDDFromCassandra")
        .set("spark.cassandra.connection.host", "127.0.0.1") // Add this line to link to Cassandra
        
        val sc = new SparkContext(sparkConf)

        val rdd = sc.cassandraTable("twitter", "users_communicate")
        
        rdd.toArray.foreach(println)
        
    }
}