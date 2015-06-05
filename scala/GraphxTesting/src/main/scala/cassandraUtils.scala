package cassandraUtils

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// Enable Cassandra-specific functions on the StreamingContext, DStream and RDD:
import com.datastax.spark.connector._

// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._


class CassandraUtils {

    val RED = "\033[1;30m"
    val ENDC = "\033[0m"

    def color(str: String, col: String): String = "%s%s%s".format(col, str, ENDC)

    /**
     * @constructor getTweetContentFromID
     *
     * Return tweet content
     *
     * @param SparkContext sc - SparkContext
     * @param String $id - tweet id
     * @return Unit
     */
    def getTweetContentFromID(sc:SparkContext, id:String): Unit = {

        println(color("\nCall getTweetContentFromID" , RED))

        val query = sc.cassandraTable("twitter", "tweet_filtered").select("tweet_text").where("tweet_id = ?", id)

        if(query.count != 0) {
            query.foreach(e => println(e.getString("tweet_text")))
        }
        else
            println("Tweet not found")
    }

    /**
     * @constructor getTweetsIDFromUser
     *
     * Return tweet id
     *
     * @param SparkContext sc - SparkContext
     * @param String $id - user (sender) id
     * @return Unit
     */
    def getTweetsIDFromUser(sc:SparkContext, id:String): Unit = {

        println(color("\nCall getTweetsIDFromUser" , RED))

        val query = sc.cassandraTable("twitter", "users_communicate").select("tweet_id").where("user_send_id = ?", id)

        if(query.count != 0)
        {
            query.foreach(e => println(e.getString("tweet_id")))
        }
        else
            println("This user does not communicate")
    }

    /**
     * @constructor getTweetsContentFromEdge
     *
     * Return an array of tweets content for a given Graph
     *
     * @param SparkContext sc - SparkContext
     * @param RDD[Edge[String]] $edge - graph's edge
     * @return Unit
     */
    def getTweetsContentFromEdge(sc:SparkContext, edge:RDD[Edge[String]]): Unit = {

        println(color("\nCall getTweetsContentFromEdge" , RED))

        // Get the tweets ID for every communication
        val tweetsID = edge.flatMap({
            case Edge(idSend, idExp, idTweet) => Seq(idTweet)
        })

        // Result will be stored in an array
        val result = ArrayBuffer[String]()

        // Queries
        for (tweet <- tweetsID.toArray) {
            val query = sc.cassandraTable("twitter", "tweet_filtered").select("tweet_text").where("tweet_id = ?", tweet)

            if(query.count != 0) {
                result += query.first.getString("tweet_text")
            }
        }

        result.foreach(println(_))
    }
}