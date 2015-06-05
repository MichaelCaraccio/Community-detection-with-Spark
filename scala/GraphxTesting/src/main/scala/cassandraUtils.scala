package cassandraUtils

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// Enable Cassandra-specific functions on the StreamingContext, DStream and RDD:
import com.datastax.spark.connector._

// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._


class CassandraUtils {
    def getTweetContentFromID(sc:SparkContext, id:String): Unit = {
        val query = sc.cassandraTable("twitter", "tweet_filtered").select("tweet_text").where("tweet_id = ?", id)

        if(query.count != 0) {
            query.foreach(e => println(e.getString("tweet_text")))
            //println(query.first.getString("tweet_text"))
        }
        else
            println("Tweet not found")
    }

    def getTweetsIDFromUser(sc:SparkContext, id:String): Unit = {

        val query = sc.cassandraTable("twitter", "users_communicate").select("tweet_id").where("user_send_id = ?", id)

        if(query.count != 0)
        {
            query.foreach(e => println(e.getString("tweet_id")))
            //println(query.first.getString("tweet_text"))
        }
        else
            println("This user does not communicate")
    }

    def getTweetsContentFromEdge(sc:SparkContext, edge:RDD[Edge[String]]): Unit = {

        //edge.collect.foreach{ case Edge(idSend, idExp, idTweet) => println(idTweet)}
        //val tweetsID = edge.collect.foreach(e => e.attr).flatten
        val tweetsID = edge.flatMap({
            case Edge(idSend, idExp, idTweet) => Seq(idTweet)
        })
        println("TweetsID: ")
        tweetsID.foreach(println(_))

        /*val query = sc.cassandraTable("twitter", "tweet_filtered").select("tweet_text").where("tweet_id = ?", id)

        if(query.count != 0)
        {
            query.foreach(e => println(e.getString("tweet_text")))
            //println(query.first.getString("tweet_text"))
        }*/
    }
}