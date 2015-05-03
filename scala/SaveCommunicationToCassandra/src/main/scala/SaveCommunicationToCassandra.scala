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
import org.apache.spark.rdd.RDD


// Useful links
// https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
// http://planetcassandra.org/getting-started-with-apache-spark-and-cassandra/
// https://bcomposes.wordpress.com/2013/02/09/using-twitter4j-with-scala-to-access-streaming-tweets/
// https://github.com/datastax/spark-cassandra-connector/blob/master/doc/5_saving.md

object SaveCommunicationToCassandra {
    def main(args: Array[String]) {

        // Display only warning messages
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)

        val filters = args
        
        // Spark configuration
        val sparkConf = new SparkConf(true)
        .setMaster("local[4]")
        .setAppName("SaveCommunicationToCassandra")
        .set("spark.cassandra.connection.host", "127.0.0.1") // Add this line to link to Cassandra
        
        // Filters by words that contains @
        val words = Array("@")
        
        // Pattern used to find users
        val pattern = new Regex("\\@\\w+")
        
        // First twitter instance : Used for stream
        val twitterstream = new TwitterFactory().getInstance()
        twitterstream.setOAuthConsumer("MCrQfOAttGZnIIkrqZ4lQA9gr", "5NnYhhGdfyqOE4pIXXdYkploCybQMzFJiQejZssK4a3mNdkCoa")
        twitterstream.setOAuthAccessToken(new AccessToken("237197078-6zwzHsuB3VY3psD5873hhU3KQ1lSVQlOXyBhDqpG", "UIMZ1aD06DObpKI741zC8wHZF8jkj1bh02Lqfl5cQ76Pl"))
        System.setProperty("twitter4j.http.retryCount", "3");
        System.setProperty("twitter4j.http.retryIntervalSecs", "10")
        System.setProperty("twitter4j.async.numThreads", "10");

        val ssc = new StreamingContext(sparkConf, Seconds(1))
        val stream = TwitterUtils.createStream(ssc, Option(twitterstream.getAuthorization()), words)
        
        // Second twitter instance : Used to query user's informations
        val twitter = new TwitterFactory().getInstance()
        twitter.setOAuthConsumer("Vb0BxXrK933CDEeQ3Myj69kkC", "q55rXOM8pQnnAyPrYhHh6LHK4IFHw0U01tfe6VDoleaxmvOL3B")
        twitter.setOAuthAccessToken(new AccessToken("237197078-iXi3ANEAUXNmoDbcbH3lvS93vDO6PvEQj3255ToL", "Skv8J9xcfhbKV2Lwddke2g7llTDwwh6S9QyAlNR6fanqY"))

        // Stream about users
        val usersStream = stream.map{status => (status.getUser.getId.toString, 
                                                status.getUser.getName.toString,
                                                status.getUser.getLang,
                                                status.getUser.getFollowersCount.toString,
                                                status.getUser.getFriendsCount.toString,
                                                status.getUser.getScreenName,
                                                status.getUser.getStatusesCount.toString)}
        
        // Stream about users
        val commStream = stream.map{status => (status.getId.toString, 
                                                status.getUser.getId.toString, 
                                                status.getUser.getName.toString,
                                                if(pattern.findFirstIn(status.getText).isEmpty){
                                                        ""
                                                    }
                                                    else
                                                    {
                                                        pattern.findFirstIn(status.getText).getOrElse("@MichaelCaraccio").tail
                                                    }
                                              )}

        // Stream about tweets
        val tweetsStream = stream.map{status => (status.getId.toString, 
                                                 status.getUser.getId.toString, 
                                                 status.getUser.getName.toString,
                                                 status.getText, 

                                                    if(pattern.findFirstIn(status.getText).isEmpty){
                                                        ""
                                                    }
                                                    else
                                                    {
                                                        pattern.findFirstIn(status.getText).getOrElse("@MichaelCaraccio").tail
                                                    },

                                                   /* if(pattern.findFirstIn(status.getText).isEmpty){
                                                        ""
                                                    }
                                                    else{
                                                        twitterstream.showUser(pattern.findFirstIn(status.getText).getOrElse("@MichaelCaraccio").tail).getId
                                                    },*/

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
        
        // Save user's informations in Cassandra
        usersStream.foreachRDD(rdd => {
            //rdd.saveToCassandra("twitter", "user_filtered", SomeColumns("user_id", "user_name", "user_lang", "user_follower_count", "user_friends_count", "user_screen_name", "user_status_count"))
            println("user added")
        })

        
        commStream.foreachRDD(rdd => {
                
            //var dest_name = "test"
            //val r3 = rdd.filter(_._2 != 0)
            println(rdd.count())
            /*if (rdd.toArray.length > 0){
                val rrrr = rdd.toList
                val dest = rrrr(0)
                

                //println(r3)
                println("ici " + rrrr)
                println("la "+ dest)
            }
            else{
                //println("pas trouvé")
            }*/
            //val parsedData = rdd.mapValues(_.toList)
            //con(parsedData)
            rdd.saveToCassandra("twitter", "users_communicate", SomeColumns("tweet_id","user_send_id","user_send_name","user_dest_name"))
            rdd.foreach {r => {
                
                println(r)
                //if (r._3 != ""){
                    //con(r._3,rdd)
                //}
                //val con = r._3
                //println(r._3)
                //println(r._1)
                //if (r._3 != ""){
                
                //}
                }}
            
            println("_________---_____---_____---___")
            //println(rdd.take(1))
            /*if(rdd.take(3) != ""){
                rdd.saveToCassandra("twitter", "users_communicate", SomeColumns("tweet_id","user_send_id","user_send_name","user_dest_name"))
            }*/
            //println("comm added")
            println("_________---_____---_____---___")

            
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
            
            //val pairs = rdd.map(_._1, _._5)
            //val cache = collection.mutable.Map[String, String] = Map(id -> 'iddd')
            
            rdd.foreach {r => {
                val tweet_id = r._1
                val sender_name = r._3
                val sender_id = r._2
                val tweet_text = r._4
                val dest_name = r._5               
                //val dest_id = r._6
                
                //val collection = Seq((tweet_id,dest_name,sender_id,sender_name))
                
                //collection.saveToCassandra("twitter", "users_communicate", SomeColumns("tweet_id","user_dest_name","user_send_id","user_send_name"))

                /*println("----------------------------------------------")
                println("Sender ID : " + sender_id)
                println("Sender Name : " + sender_name)
                println("Tweet : " + tweet_text)
                println("Dest name :" + dest_name)
                //println("Dest ID : " + dest_id)
                println("----------------------------------------------")*/

            }}
            println("tweet added")
        })

        ssc.start()
        ssc.awaitTermination()
    }
    
    //def con(rdd: RDD[(String, String, String, String)]) : String = {
        /*println(rdd.(0))
        //return "con"
        
        val con = rdd.toArray
        con.foreach {r => {
                
                println(rdd)
                if (r._3 != ""){
                    return "lol"
                }
            return "pas lol"
                //val con = r._3
                //println(r._3)
                //println(r._1)
                //if (r._3 != ""){
                
                //}
                }}*/
        //rdd.saveToCassandra("twitter", "users_communicate", SomeColumns("tweet_id","user_send_id","user_send_name","user_dest_name"))
   // }
}