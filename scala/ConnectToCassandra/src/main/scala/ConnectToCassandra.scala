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

import com.datastax.spark.connector._ 
import com.datastax.spark.connector.streaming._

import scala.util.matching.Regex


// Useful links
// https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
// http://planetcassandra.org/getting-started-with-apache-spark-and-cassandra/
// https://bcomposes.wordpress.com/2013/02/09/using-twitter4j-with-scala-to-access-streaming-tweets/
// https://github.com/datastax/spark-cassandra-connector/blob/master/doc/5_saving.md

//put your keys and creds here 
object Util {
  val config = new twitter4j.conf.ConfigurationBuilder()
    .setOAuthConsumerKey("MCrQfOAttGZnIIkrqZ4lQA9gr")
    .setOAuthConsumerSecret("5NnYhhGdfyqOE4pIXXdYkploCybQMzFJiQejZssK4a3mNdkCoa")
    .setOAuthAccessToken("237197078-6zwzHsuB3VY3psD5873hhU3KQ1lSVQlOXyBhDqpG")
    .setOAuthAccessTokenSecret("UIMZ1aD06DObpKI741zC8wHZF8jkj1bh02Lqfl5cQ76Pl")
    .build
}

object ConnectToCassandra {
    def main(args: Array[String]) {

        // Display only warning messages
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)

        val filters = args
        
        // Set the system properties so that Twitter4j library used by twitter stream
        // can use them to generat OAuth credentials
        /*System.setProperty("twitter4j.oauth.consumerKey", "MCrQfOAttGZnIIkrqZ4lQA9gr")
        System.setProperty("twitter4j.oauth.consumerSecret", "5NnYhhGdfyqOE4pIXXdYkploCybQMzFJiQejZssK4a3mNdkCoa")
        System.setProperty("twitter4j.oauth.accessToken", "237197078-6zwzHsuB3VY3psD5873hhU3KQ1lSVQlOXyBhDqpG")
        System.setProperty("twitter4j.oauth.accessTokenSecret", "UIMZ1aD06DObpKI741zC8wHZF8jkj1bh02Lqfl5cQ76Pl")
        System.setProperty("twitter4j.http.retryCount", "3");
        System.setProperty("twitter4j.http.retryIntervalSecs", "10")
        System.setProperty("twitter4j.async.numThreads", "1");
        System.setProperty("twitter4j.http.useSSL", "false");*/
        //System.setProperty("async.numThreads", "10");

        val sparkConf = new SparkConf(true)
        .setMaster("local[4]")
        .setAppName("ConnectToCassandra")
        .set("spark.cassandra.connection.host", "127.0.0.1") // Add this line to link to Cassandra


        
        // Filters by words
        val words = Array("@")
        
        
        //val austinBox = Array(Array(-97.8,30.25),Array(-97.65,30.35))
        //val twitterStream = new TwitterStreamFactory(Util.config).getInstance
        //twitterStream.filter(new FilterQuery().locations(austinBox))
        
        val twitterstream = new TwitterFactory().getInstance()
        twitterstream.setOAuthConsumer("MCrQfOAttGZnIIkrqZ4lQA9gr", "5NnYhhGdfyqOE4pIXXdYkploCybQMzFJiQejZssK4a3mNdkCoa")
        twitterstream.setOAuthAccessToken(new AccessToken("237197078-6zwzHsuB3VY3psD5873hhU3KQ1lSVQlOXyBhDqpG", "UIMZ1aD06DObpKI741zC8wHZF8jkj1bh02Lqfl5cQ76Pl"))

        val ssc = new StreamingContext(sparkConf, Seconds(1))
        val stream = TwitterUtils.createStream(ssc, Option(twitterstream.getAuthorization()), words)
        
        
        // Twitter Authentication credentials
        /*val consumerKey = Play.current.configuration.getString("MCrQfOAttGZnIIkrqZ4lQA9gr").get
        val consumerSecret = Play.current.configuration.getString("5NnYhhGdfyqOE4pIXXdYkploCybQMzFJiQejZssK4a3mNdkCoa").get
        val accessToken = Play.current.configuration.getString("237197078-6zwzHsuB3VY3psD5873hhU3KQ1lSVQlOXyBhDqpG").get
        val accessTokenSecret = Play.current.configuration.getString("UIMZ1aD06DObpKI741zC8wHZF8jkj1bh02Lqfl5cQ76Pl").get*/

        // Authorising with your Twitter Application credentials
        val twitter = new TwitterFactory().getInstance()
        twitter.setOAuthConsumer("Vb0BxXrK933CDEeQ3Myj69kkC", "q55rXOM8pQnnAyPrYhHh6LHK4IFHw0U01tfe6VDoleaxmvOL3B")
        twitter.setOAuthAccessToken(new AccessToken("237197078-iXi3ANEAUXNmoDbcbH3lvS93vDO6PvEQj3255ToL", "Skv8J9xcfhbKV2Lwddke2g7llTDwwh6S9QyAlNR6fanqY"))
        
        
        
        
        
        

        //val twcitter = new TwitterFactory().getInstance
        //val statuses = twitter.search(new Query("#caca")).getTweets 
        //val anus = twitter.showUser("2948436072")
        val pattern = new Regex("\\@\\w+")

        // Stream about users
        val usersStream = stream.map{status => (status.getUser.getId.toString, 
                                                status.getUser.getName.toString,
                                                status.getUser.getLang,
                                                status.getUser.getFollowersCount.toString,
                                                status.getUser.getFriendsCount.toString,
                                                status.getUser.getScreenName,
                                                status.getUser.getStatusesCount.toString)}

        // Stream about tweets
        val tweetsStream = stream.map{status => (status.getId.toString, 
                                                 status.getUser.getId.toString, 
                                                 status.getText, 
                                                 twitter.showUser(pattern.findFirstIn(status.getText).getOrElse("@MichaelCaraccio").tail),
                                                 status.getRetweetCount.toString,
                                                 new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(status.getCreatedAt),

                                                 Option(status.getGeoLocation) match {
                                                     case Some(theValue) => 
                                                     status.getGeoLocation.getLongitude.toString
                                                     case None           => 
                                                     ""
                                                 }, 

                                                 Option(status.getGeoLocation) match {
                                                     case Some(theValue) => 
                                                     status.getGeoLocation.getLatitude.toString
                                                     case None           => 
                                                     ""
                                                 }
                                                )}

        //val twitter = TwitterFactory.getSingleton()
        
        // Save user's informations in Cassandra
        usersStream.foreachRDD(rdd => {
            //rdd.saveToCassandra("twitter", "user_filtered", SomeColumns("user_id", "user_name", "user_lang", "user_follower_count", "user_friends_count", "user_screen_name", "user_status_count"))
            println("user added")
        })

        // Save tweet's informations in Cassandra
        tweetsStream.foreachRDD(rdd => {
            //rdd.saveToCassandra("twitter", "tweet_filtered", SomeColumns("tweet_id", "user_id", "tweet_text", "tweet_retweet", "tweet_create_at", "user_longitude", "user_latitude"))
            
            
           /* val twitter = new TwitterFactory().getInstance
            val userName = twitter.getScreenName

            val statuses = twitter.getMentionsTimeline.take(2)
  
            statuses.foreach { status => {
                val statusAuthor = status.getUser.getScreenName
                val mentionedEntities = status.getUserMentionEntities.map(_.getScreenName).toList
                val participants = (statusAuthor :: mentionedEntities).toSet - userName
                val text = participants.map(p=>"@"+p).mkString(" ") + " OK."
                val reply = new StatusUpdate(text).inReplyToStatusId(status.getId)
                println("Replying: " + text)
                //twitter.updateStatus(reply)
                println("DAT BITCH" + mentionedEntities)
                println("DAT BITCH2" + reply)
            }}*/
            
            
            
            rdd.foreach {r => {
                // ****************************************		
                // TEXT		
                // ****************************************	
                
                // Pattern cherchant les @
                //val pattern = new Regex("\\@\\w+")
                
                // Tweet
                //val tweet_text_some = r._4	
                val sender_id = r._2
                val con = r._3
                val wesh = r._4
                
                
                //println("YOLO:" + (pattern findFirstIn con).tail)
                
                println("YOLO:" + pattern.findFirstIn(con).getOrElse("no match").tail)
                println("TADAM:" + wesh)
                
                //statuses.foreach(status => println(status.getText + "\n"))
                
                //val test = twitter.getUser(sender_id)
                
                //println("DAT BITCH2" + reply)

                
                println("Sender ID : " + sender_id)
                println("Sender Name : " + sender_id)
                //println("DEst ID : " + tweet_text_some)
                //println("Dest Name" + (pattern findAllIn tweet_text_some).mkString(","))
            }}
            //println(rdd->"tweet_text".value)
            println("tweet added")
        })

        ssc.start()
        ssc.awaitTermination()
    }
}