import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.twitter
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._

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

import org.apache.spark.rdd.RDD

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
    /*def getTweets(ssc: StreamingContext, keywords: Array[String]) = {
        val stream = TwitterUtils.createStream(ssc, None, keywords)

        val tweets = stream.map(status => {
            val hashmap = new scala.collection.mutable.HashMap[String, Object]()
            hashmap.put("id", status.getUser.getId.toString)
            hashmap.put("user_name", status.getUser.getName)
            hashmap.put("user_lang", status.getUser.getLang)
            hashmap.put("text", status.getText)
            hashmap.put("create_at", new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").format(status.getCreatedAt))
            (status.getUser.getId.toString, hashmap)
        }).window(Seconds(3600), Seconds(2))

        tweets.foreachRDD(rdd => {
            rdd.foreach {r => {
                println("keywords = " + keywords.mkString(","))
                println(r)
            }
                        }
        })

        tweets
    }*/

    def main(args: Array[String]) {

        // Suppression des messages INFO
        //Logger.getLogger("org").setLevel(Level.WARN)
        //Logger.getLogger("akka").setLevel(Level.WARN)

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

        //val databaseContext = new SparkContext("spark://cassandra-server:7077", "demo", sparkConf)  
        val ssc = new StreamingContext(sparkConf, Seconds(1))
        val stream = TwitterUtils.createStream(ssc, None)


        val tweets = stream.map(status => {
            val hashmap = new scala.collection.mutable.HashMap[String, Object]()
            hashmap.put("id", status.getUser.getId.toString)
            hashmap.put("user_name", status.getUser.getName)
            hashmap.put("user_lang", status.getUser.getLang)
            hashmap.put("text", status.getText)
            hashmap.put("create_at", new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").format(status.getCreatedAt))
            (status.getUser.getId.toString, hashmap)
        })
        
        //val rdd = ssc.cassandraTable("demo", "users").select("lastname").where("lastname = ?", "Doe")
        val rdd = ssc.cassandraTable("demo", "users")

        //val rdd = ssc.cassandraTable("demo", "users").toArray
        
        //case class Person(lastname: String, age: Int,city: String,email: String,firstname: String)
        //ssc.cassandraTable[Person]("demo", "users").registerAsTable("users")
        //val adults = ssc.sql("SELECT * FROM users")
        println("-------")
        println(rdd)
        rdd.toArray.foreach(println)
        //rdd.toArray().foreach(println)
        //rdd.toArray.foreach(println)
        println("-------")
        //println(adults)
        println("-------")
        
        // DEMAIN ESSAYER LIEN CI_DESSOUS
        // http://formacionhadoop.com/aulavirtual/mod/forum/discuss.php?d=23


        tweets.foreachRDD(rdd => {
            rdd.foreach {r => {
                
                //println(">>> user_name=" + r._2.get("user_name").foreach( i => println("Got: " + i)))
                
                // http://stackoverflow.com/questions/9389902/scala-mapget-and-the-return-of-some
                // Le foreach( i => println("xxx: " + i)) Permet d'éviter le Some() et de prendre directement sa valeur
                // r._2.get("user_name").foreach( i => println("user_name: " + i))
                //val con = r._2.get("user_name").foreach( i => return i)
                
                // http://danielwestheide.com/blog/2012/12/19/the-neophytes-guide-to-scala-part-5-the-option-type.html
                // http://www.scala-lang.org/old/node/107
                val user_name_some = r._2.get("user_name")
                val user_name = user_name_some match {
                  case None => "No key with that name!"
                  case Some(x) => x
                }
                
                val text_some = r._2.get("text")
                val text = text_some match {
                  case None => "No key with that name!"
                  case Some(x) => x
                }
                
                val id_some = r._2.get("id")
                val id = id_some match {
                  case None => "No key with that name!"
                  case Some(x) => x
                }
                
                println("\nID: " + id)
                println("\tUser_name: " + user_name)
                println("\tText: " + text)

            }
                        }
        })
        ssc.start()
        ssc.awaitTermination()
    }
}