import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.streaming.twitter
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.twitter.TwitterUtils

import org.apache.spark.SparkConf

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._

import twitter4j.TwitterFactory
import twitter4j.auth.AccessToken
import twitter4j._
import collection.JavaConversions._

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math._

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

object SaveCommunicationToCassandra{

    private val defaultSeed = 0xadc83b19L

    /**
     * @constructor murmurHash64A
     *
     *
     * @param
     * @param
     * @return Long
     *
     */
    def murmurHash64A(data: Seq[Byte], seed: Long = defaultSeed): Long = {
        val m = 0xc6a4a7935bd1e995L
        val r = 47

        val f: Long => Long = m.*
        val g: Long => Long = x => x ^ (x >>> r)

        val h = data.grouped(8).foldLeft(seed ^ f(data.length)) { case (y, xs) =>
            val k = xs.foldRight(0L)((b, x) => (x << 8) + (b & 0xff))
            val j: Long => Long = if (xs.length == 8) f compose g compose f else identity
            f(y ^ j(k))
        }
        (g compose f compose g)(h)
    }
    
    def main(args: Array[String]) {
        
        // Display only warning and infos messages
        //Logger.getLogger("org").setLevel(Level.ERROR)
        //Logger.getLogger("akka").setLevel(Level.ERROR)
        
        // Not displaying infos messages
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        
        // Spark configuration
        val sparkConf = new SparkConf(true)
        .setMaster("local[4]")
        .setAppName("SaveCommunicationToCassandra")
        .set("spark.cassandra.connection.host", "127.0.0.1") // Link to Cassandra
        
        // Filters by words that contains @
        val words = Array(" @")
        
        // Pattern used to find users
        val pattern = new Regex("\\@\\w{3,}")
        val patternURL = new Regex("(http|ftp|https)://[A-Za-z0-9-_]+.[A-Za-z0-9-_:%&?/.=]+")
        val patternSmiley = new Regex("((?::|;|=)(?:-)?(?:\\)|D|P|3|O))")
        
        // First twitter instance : Used for stream
        val twitterstream = new TwitterFactory().getInstance()
        twitterstream.setOAuthConsumer("MCrQfOAttGZnIIkrqZ4lQA9gr", "5NnYhhGdfyqOE4pIXXdYkploCybQMzFJiQejZssK4a3mNdkCoa")
        twitterstream.setOAuthAccessToken(new AccessToken("237197078-6zwzHsuB3VY3psD5873hhU3KQ1lSVQlOXyBhDqpG", "UIMZ1aD06DObpKI741zC8wHZF8jkj1bh02Lqfl5cQ76Pl"))
        
        System.setProperty("twitter4j.http.retryCount", "3");
        System.setProperty("twitter4j.http.retryIntervalSecs", "10")
        System.setProperty("twitter4j.async.numThreads", "10");

        val ssc = new StreamingContext(sparkConf, Seconds(1))
        val stream = TwitterUtils.createStream(ssc, Option(twitterstream.getAuthorization()), words)

        // Stream about users
        val usersStream = stream.map{status => (
            status.getUser.getId.toString,
            abs(murmurHash64A(status.getUser.getScreenName.getBytes)),
            status.getUser.getName.toString,
            status.getUser.getLang,
            status.getUser.getFollowersCount.toString,
            status.getUser.getFriendsCount.toString,
            status.getUser.getScreenName,
            status.getUser.getStatusesCount.toString)}
        
        
        // Stream about communication between two users
        val commStream = stream.map{status => (
            status.getId.toString, //tweet_id
            status.getUser.getId.toString, // user_send_twitter_ID
            abs(murmurHash64A(status.getUser.getScreenName.getBytes)), // user_send_local_ID
            if(pattern.findFirstIn(status.getText).isEmpty)
            {
                    ""
            }
            else
            {
                pattern.findFirstIn(status.getText).getOrElse("@MichaelCaraccio").tail
            },
           status.getText,
           status.getUser.getLang
        )}
        
        

        // Stream about tweets
        val tweetsStream = stream.map{status => (
            status.getId.toString,
            status.getUser.getId.toString,
            abs(murmurHash64A(status.getUser.getScreenName.getBytes)),
            new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(status.getCreatedAt),
            status.getRetweetCount.toString,
            status.getText
        )}
        
        
        // ************************************************************
        // Save user's informations in Cassandra
        // ************************************************************
        usersStream.foreachRDD(rdd => {
            rdd.saveToCassandra("twitter", "user_filtered", SomeColumns("user_twitter_id", "user_local_id", "user_name", "user_lang", "user_follow_count", "user_friends_count", "user_screen_name", "user_status_count"))

            println("Users saved : " + rdd.count())
        })
        
        // ************************************************************
        // Save communication's informations in Cassandra
        // ************************************************************
        commStream.foreachRDD(rdd => {
            // Getting current context
            val currentContext = rdd.context

            // RDD -> Array()
            val tabValues = rdd.collect()
            
            // For each tweets in RDD
            for(item <- tabValues.toArray) { 
                
                // Avoid single @ in message
                if(item._4 != "" && (item._6 == "en" || item._6 == "en-gb")){
                    
                    // Find multiple dest
                    val matches = pattern.findAllIn(item._5).toArray
                    
                    // For each receiver in tweet
                    matches.foreach{destName => {
                        var user_dest_name = destName.drop(1)

                        // TODO : Optimize save to cassandra with concatenate seq and save it when the loop is over
                        val collection = currentContext.parallelize(Seq((item._1, item._2,item._3, abs(murmurHash64A(user_dest_name.getBytes)))))
                        
                        collection.saveToCassandra(
                            "twitter", 
                            "users_communicate",
                            SomeColumns(
                                "tweet_id",
                                "user_send_twitter_id",
                                "user_send_local_id",
                                "user_dest_id"))
                    }}
                }
            }
            
            println("Comm saved : " + rdd.count())
        })
        
        
        // ************************************************************
        // Save tweet's informations in Cassandra
        // ************************************************************
        tweetsStream.foreachRDD(rdd => {

            // Getting current context
            val currentContext = rdd.context
            
            // RDD -> Array()
            val tabValues = rdd.collect()
            
            /*var test = rdd.map{status => (status._1,
                                          status._2,
                                          patternURL.replaceAllIn(status._3, ""),
                                          status._4,
                                          status._5, 
                                          status._6, 
                                          status._7)}*/
            
            // For each tweets in RDD
            for(item <- tabValues.toArray) { 
                
                // New tweet value
                var newTweet = patternURL.replaceAllIn(item._6, "")
                newTweet = patternSmiley.replaceAllIn(newTweet, "")
                
                val collection = currentContext.parallelize(Seq((item._1, item._2, item._3, item._4, item._5, newTweet)))
                
                collection.saveToCassandra(
                    "twitter", 
                    "tweet_filtered",
                    SomeColumns("tweet_id", 
                                "user_twitter_id",
                                "user_local_id",
                                "tweet_create_at",
                                "tweet_retweet",
                                "tweet_text"
                                ))
            }
            
            println("Tweets saved : " + rdd.count())
        })

        ssc.start()
        ssc.awaitTermination()
    }
}