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

import org.apache.spark.{SparkContext,SparkConf}

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
        //val databaseContext = new SparkContext("spark://cassandra-server:7077", "demo", sparkConf)  
        val ssc = new StreamingContext(sparkConf, Seconds(1))
        val stream = TwitterUtils.createStream(ssc, None, words)
        
        
        /*case class FoodToUserIndex(user_id: String, user_name: String, user_lang: String, user_follower_count: String, user_friends_count: String, user_screen_name: String, user_status_count: String)
        
        
        val user_table = ssc.cassandraTable("twitter", "tweet_filtered")
        
        val food_index = user_table.map(r => new FoodToUserIndex("1","2","3","4","5","6","77"))
        
        food_index.saveToCassandra("twitter", "tweet_filtered", SomeColumns("user_id", "user_name", "user_lang", "user_follower_count", "user_friends_count", "user_screen_name", "user_status_count"))*/
        

        //food_index.saveToCassandra("tutorial", "food_to_user_index")

       /* val conf = new SparkConf(true)
        .set("spark.cassandra.connection.host", "127.0.0.1")
        val sc = new SparkContext("spark://127.0.0.1:7077", "ConnectToCassandra", conf)*/


        //val con = stream.map (status => status.getUser.getId(),coucou =>status.getUser.getId())
        //con.print()
        
        //val lines4 = Map(line => ("1","2","3","4","5","6","7"))
        //lines4.saveToCassandra("twitter", "user_filtered", SomeColumns("user_id", "user_name", "user_lang", "user_follower_count", "user_friends_count", "user_screen_name", "user_status_count"))

       val usersStream = stream.map{status => (status.getUser.getId.toString, status.getUser.getName.toString, status.getUser.getLang,status.getUser.getFollowersCount.toString,status.getUser.getFriendsCount.toString,status.getUser.getScreenName,status.getUser.getStatusesCount.toString)}
        
    /*    val some = Option(status.getGeoLocation)
            some match {
                case Some(theValue) => 
                    var lat = status.getGeoLocation.getLatitude.toString
                    var lng = status.getGeoLocation.getLongitude.toString
                case None           => 
                    var lat = ""
                    var lng = ""
            }*/
        
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

        
    /*    val tweets = stream.map(status => {
            val hashmap = new scala.collection.mutable.HashMap[String, Object]()
            hashmap.put("user_id", status.getUser.getId.toString)
            hashmap.put("tweet_id", status.getId.toString)
            hashmap.put("user_follower_count", status.getUser.getFollowersCount.toString)
            hashmap.put("user_friends_count",status.getUser.getFriendsCount.toString)
            hashmap.put("user_screen_name", status.getUser.getScreenName) 
            hashmap.put("user_status_count", status.getUser.getStatusesCount.toString) 
            hashmap.put("tweet_retweet_count", status.getRetweetCount.toString) 

            val some = Option(status.getGeoLocation)
            some match {
                case Some(theValue) => 
                                    hashmap.put("user_latitude", status.getGeoLocation.getLatitude.toString)
                                    hashmap.put("user_longitude", status.getGeoLocation.getLongitude.toString)
                case None           => 
                                    hashmap.put("user_latitude", "")
                                    hashmap.put("user_longitude", "")
            }
            
            hashmap.put("user_name", status.getUser.getName.toString)
            hashmap.put("user_lang", status.getUser.getLang)
            hashmap.put("tweet_text", status.getText)
            hashmap.put("tweet_create_at", new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").format(status.getCreatedAt))
            (status.getUser.getId.toString, hashmap)
        })*/
        

        //val rdd = ssc.cassandraTable("demo", "users").select("lastname").where("lastname = ?", "Doe")
        //val rdd_tweet = ssc.cassandraTable("twitter", "tweet_filtered")
        //val rdd_user = ssc.cassandraTable("twitter", "user_filtered")
        //val rdd = ssc.cassandraTable("demo", "users").toArray

        //case class Person(lastname: String, age: Int,city: String,email: String,firstname: String)
        //ssc.cassandraTable[Person]("demo", "users").registerAsTable("users")
        //val adults = ssc.sql("SELECT * FROM users")
        println("-------")
        //println(rdd)
       // rdd_tweet.toArray.foreach(println)
        //rdd.toArray().foreach(println)
        //rdd.toArray.foreach(println)
        println("-------")
        //println(adults)
        //println("-------")

        // DEMAIN ESSAYER LIEN CI_DESSOUS
        // http://formacionhadoop.com/aulavirtual/mod/forum/discuss.php?d=23

        //tweets.saveToCassandra("twitter", "tweet_filtered", Seq("status", "user_id", "tweet_create_at", "tweet_text", "tweet_retweet_count", "user_longitude", "user_latitude"))
       // val test = Seq(1,1,"test1","test2","test3","test4","test55")
       // val pute = test.map({case (id,uri,count) => (id,uri)->count})
        //test.saveToCassandra("twitter", "user_filtered", SomeColumns("user_id", "user_name", "user_lang", "user_follower_count", "user_friends_count", "user_screen_name", "user_status_count"))
        
        usersStream.foreachRDD(rdd => {
            rdd.saveToCassandra("twitter", "user_filtered", SomeColumns("user_id", "user_name", "user_lang", "user_follower_count", "user_friends_count", "user_screen_name", "user_status_count"))
            println("user added")
        })
        
        tweetsStream.foreachRDD(rdd => {
            rdd.saveToCassandra("twitter", "tweet_filtered", SomeColumns("tweet_id", "user_id", "tweet_text", "tweet_retweet", "tweet_create_at", "user_longitude", "user_latitude"))
            println("tweet added")
        })
        
        
     /*   tweets.foreachRDD(rdd => {            
            rdd.foreach {r => {   

                    // ****************************************
                    // USER_NAME
                    // ****************************************
                    val user_name_some = r._2.get("user_name")
                    val user_name = user_name_some match {
                        case None => ""
                        case Some(x) => x
                    }

                    // ****************************************
                    // TEXT
                    // ****************************************
                    val tweet_text_some = r._2.get("tweet_text")
                    val tweet_text = tweet_text_some match {
                        case None => ""
                        case Some(x) => x
                    }

                    // ****************************************
                    // USER ID
                    // ****************************************
                    val user_id_some = r._2.get("user_id")
                    val user_id = user_id_some match {
                        case None => ""
                        case Some(x) => x
                    }
                
                    // ****************************************
                    // TWEET ID
                    // ****************************************
                    val tweet_id_some = r._2.get("tweet_id")
                    val tweet_id = tweet_id_some match {
                        case None => ""
                        case Some(x) => x
                    }

                    // ****************************************
                    // FOLLOWER COUNT
                    // ****************************************
                    val user_follower_count_some = r._2.get("user_follower_count")
                    val user_follower_count = user_follower_count_some match {
                        case None => ""
                        case Some(x) => x
                    }

                    // ****************************************
                    // FRIENDS COUNT
                    // ****************************************
                    val user_friends_count_some = r._2.get("user_friends_count")
                    val user_friends_count = user_friends_count_some match {
                        case None => ""
                        case Some(x) => x
                    }

                    // ****************************************
                    // SCREEN NAME
                    // ****************************************
                    val user_screen_name_some = r._2.get("user_screen_name")
                    val user_screen_name = user_screen_name_some match {
                        case None => ""
                        case Some(x) => x
                    }

                    // ****************************************
                    // STATUS COUNT
                    // ****************************************
                    val user_status_count_some = r._2.get("user_status_count")
                    val user_status_count = user_status_count_some match {
                        case None => ""
                        case Some(x) => x
                    }

                    // ****************************************
                    // RETWEET COUNT
                    // ****************************************
                    val tweet_retweet_count_some = r._2.get("tweet_retweet_count")
                    val tweet_retweet_count = tweet_retweet_count_some match {
                        case None => ""
                        case Some(x) => x
                    }

                    // ****************************************
                    // USER LATITUDE
                    // ****************************************
                    val user_latitude_some = r._2.get("user_latitude")
                    val user_latitude = user_latitude_some match {
                        case None => ""
                        case Some(x) => x
                    }

                    // ****************************************
                    // USER LONGITUDE
                    // ****************************************
                    val user_longitude_some = r._2.get("user_longitude")
                    val user_longitude = user_longitude_some match {
                        case None => ""
                        case Some(x) => x
                    }

                    // ****************************************
                    // USER LANG
                    // ****************************************
                    val user_lang_some = r._2.get("user_lang")
                    val user_lang = user_lang_some match {
                        case None => ""
                        case Some(x) => x
                    }

                    // ****************************************
                    // CREATE AT
                    // ****************************************
                    val tweet_create_at_some = r._2.get("tweet_create_at")
                    val tweet_create_at = tweet_create_at_some match {
                        case None => ""
                        case Some(x) => x
                    }
  */
                
                
                  /*  val tweet_collection = sc.parallelize(Seq(tweet_id, user_id, tweet_create_at, tweet_text, tweet_retweet_count, user_longitude, user_latitude))
                    tweet_collection.saveToCassandra("twitter", "tweet_filtered", SomeColumns("tweet_id", "user_id", "tweet_text", "tweet_retweet", "tweet_create_at", "user_longitude", "user_latitude"))
                
                    val user_collection = rdd.parallelize(Seq(user_id, user_name, user_lang, user_follower_count, user_friends_count, user_screen_name, user_status_count))
                
                    user_collection.saveToCassandra("twitter", "user_filtered", SomeColumns("user_id", "user_name", "user_lang", "user_follower_count", "user_friends_count", "user_screen_name", "user_status_count"))*/
                
               /* ssc.saveToCassandra("twitter", "tweet_filtered", Seq(tweet_id, user_id, tweet_create_at, tweet_text, tweet_retweet_count, user_longitude, user_latitude))
                
                ssc.saveToCassandra("twitter", "user_filtered", Seq(user_id, user_name, user_lang, user_follower_count, user_friends_count, user_screen_name, user_status_count))
                */
                
                  /*  println("\n\ntweet_id: " + tweet_id)
                    println("user_id: " + user_id)
                    println("\ttweet_Create at: " + tweet_create_at)
                    println("\ttweet_Text: " + tweet_text)
                    println("\ttweet_Retweet count: " + tweet_retweet_count)

                    println("\tUser_name: " + user_name)
                    println("\tFollower count: " + user_follower_count)
                    println("\tFriends count: " + user_friends_count)
                    println("\tScreen name: " + user_screen_name)
                    println("\tStatus count: " + user_status_count)
                    println("\tLatitude: " + user_latitude)
                    println("\tLongitude: " + user_longitude)
                    println("\tUser lang: " + user_lang)*/
                
        /*        }
            }
        })*/
        ssc.start()
        ssc.awaitTermination()
    }
}