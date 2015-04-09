import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.twitter
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._

import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils

import twitter4j.TwitterFactory
import twitter4j.auth.AccessToken

import org.apache.log4j.Logger
import org.apache.log4j.Level

import com.datastax.spark.connector._ 
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.cql.CassandraConnector

//import org.apache.spark.{SparkContext,SparkConf}

//import org.apache.spark.rdd.RDD

/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 * Run this on your local machine as
 *
 */

// https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
// http://planetcassandra.org/getting-started-with-apache-spark-and-cassandra/
// https://bcomposes.wordpress.com/2013/02/09/using-twitter4j-with-scala-to-access-streaming-tweets/
object ConnectToCassandra {
    def main(args: Array[String]) {

        // Suppression des messages INFO
        Logger.getLogger("org").setLevel(Level.WARN)
        Logger.getLogger("akka").setLevel(Level.WARN)

        val filters = args
        // Set the system properties so that Twitter4j library used by twitter stream
        // can use them to generat OAuth credentials
        System.setProperty("twitter4j.oauth.consumerKey", "MCrQfOAttGZnIIkrqZ4lQA9gr")
        System.setProperty("twitter4j.oauth.consumerSecret", "5NnYhhGdfyqOE4pIXXdYkploCybQMzFJiQejZssK4a3mNdkCoa")
        System.setProperty("twitter4j.oauth.accessToken", "237197078-6zwzHsuB3VY3psD5873hhU3KQ1lSVQlOXyBhDqpG")
        System.setProperty("twitter4j.oauth.accessTokenSecret", "UIMZ1aD06DObpKI741zC8wHZF8jkj1bh02Lqfl5cQ76Pl")

        val sparkConf = new SparkConf(true)
        .setMaster("local[4]")
        .setAppName("ConnectToCassandra")
        .set("spark.cassandra.connection.host", "127.0.0.1") //Celle ligne est ajouté pour cassandra
        //.set("spark.cassandra.auth.username", "cassandra")
        //.set("spark.cassandra.auth.password", "cassandra")
        //.set("spark.cassandra.connection.native.port", "7077")
        //val sc = new SparkContext(conf)
        val words = Array("Michael")

        val ssc = new StreamingContext(sparkConf, Seconds(1))
        val stream = TwitterUtils.createStream(ssc, None, words)



        val usersStream = stream.map{status => (status.getUser.getId.toString, status.getUser.getName.toString, status.getUser.getLang,status.getUser.getFollowersCount.toString,status.getUser.getFriendsCount.toString,status.getUser.getScreenName,status.getUser.getStatusesCount.toString)}

        val tweetsStream = stream.map{status => (status.getId.toString, 
                                                 status.getUser.getId.toString, 
                                                 status.getText, 
                                                 status.getRetweetCount.toString, 
                                                 new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(status.getCreatedAt),

                                                 Option(status.getGeoLocation) match {
                                                     case Some(theValue) => 
                                                     status.getGeoLocation.getLongitude.toString
                                                     case None           => 
                                                     ""
                                                 }
                                                 , 

                                                 Option(status.getGeoLocation) match {
                                                     case Some(theValue) => 
                                                     status.getGeoLocation.getLatitude.toString
                                                     case None           => 
                                                     ""
                                                 }
                                                )}


     

        usersStream.foreachRDD(rdd => {
            rdd.saveToCassandra("twitter", "user_filtered", SomeColumns("user_id", "user_name", "user_lang", "user_follower_count", "user_friends_count", "user_screen_name", "user_status_count"))
            println("user added")
        })

        tweetsStream.foreachRDD(rdd => {
            rdd.saveToCassandra("twitter", "tweet_filtered", SomeColumns("tweet_id", "user_id", "tweet_text", "tweet_retweet", "tweet_create_at", "user_longitude", "user_latitude"))
            println("tweet added")
        })


        ssc.start()
        ssc.awaitTermination()
    }
}