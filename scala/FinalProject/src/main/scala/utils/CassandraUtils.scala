package utils

import scala.collection.mutable.ArrayBuffer

// Enable Cassandra-specific functions on the StreamingContext, DStream and RDD:
import com.datastax.spark.connector._

// To make some of the examples work we will also need RDD
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext


class CassandraUtils {

    val RED = "\033[1;30m"
    val ENDC = "\033[0m"

    /**
     * @constructor getTweetContentFromID
     *
     *              Return tweet content
     *
     * @param SparkContext sc - SparkContext
     * @param String $id - tweet id
     * @return Unit
     */
    def getTweetContentFromID(sc: SparkContext, id: String): String = {

        println(color("\nCall getTweetContentFromID", RED))

        val query = sc.cassandraTable("twitter", "tweet_filtered").select("tweet_text").where("tweet_id = ?", id)

        if (query.collect().length != 0) {
            query.first().getString("tweet_text")
        }
        else
            "Tweet not found"
    }

    def color(str: String, col: String): String = "%s%s%s".format(col, str, ENDC)

    /**
     * @constructor getTweetsIDFromUser
     *
     *              Return tweet id
     *
     * @param SparkContext sc - SparkContext
     * @param String $id - user (sender) id
     * @return Unit
     */
    def getTweetsIDFromUser(sc: SparkContext, id: String): ArrayBuffer[String] = {

        println(color("\nCall getTweetsIDFromUser", RED))
        println("Tweets found:")

        val query = sc.cassandraTable("twitter", "users_communicate").select("tweet_id").where("user_send_local_id = ?", id)

        // Result will be stored in an array
        var result = ArrayBuffer[String]()

        if (query.collect().length != 0) {
            result += query.first().getString("tweet_id")
        }

        // Display result
        result.foreach(println(_))

        // Return
        result
    }

    /**
     * @constructor getTweetsContentFromEdge
     *
     *              Return an array of tweets content for a given Graph
     *
     * @param SparkContext sc - SparkContext
     * @param RDD[Edge[String]] $edge - graph's edge
     * @return Unit
     */
    def getTweetsContentFromEdge(sc: SparkContext, edge: RDD[Edge[String]], displayResult: Boolean): RDD[String] = {

        println(color("\nCall getTweetsContentFromEdge", RED))

        // Get the tweets ID for every communication
        val tweetsID = edge.flatMap({
            case Edge(idSend, idExp, idTweet) => Seq(idTweet)
        })

        // Result will be stored in an array
        var result = ArrayBuffer[String]()

        // Queries
        for (tweet <- tweetsID.collect()) {
            val query = sc.cassandraTable("twitter", "tweet_filtered").select("tweet_text").where("tweet_id = ?", tweet)

            if (query.collect().length != 0) {
                result += query.first().getString("tweet_text")
            }
        }

        // Display results
        if (displayResult) {
            result.foreach(println(_))
        }

        // return
        sc.parallelize(result)
    }

    def getAllCommunications(sc: SparkContext): (RDD[(VertexId, (String))], RDD[Edge[String]]) = {
        println(color("\nCall getAllCommunications", RED))

        // Collection of vertices (contains users)
        val collectionVertices = ArrayBuffer[(Long, String)]()

        var query = sc.cassandraTable("twitter", "user_filtered").select("user_local_id", "user_screen_name").toArray()

        println("Query 1 ok")
        // Save result to ArrayBuffer
        /*if (query.collect().length != 0) {
            collectionVertices += ((query.first().getString("user_local_id").toLong, query.first().getString("user_local_id").toString))
        }*/

        //collectionVertices.foreach(println(_))

        println("Query 1 Collect ok")



        // Collection of edges (contains communications between users)
        val collectionEdge = ArrayBuffer[Edge[String]]()


        query = sc.cassandraTable("twitter", "users_communicate").select("user_send_local_id", "user_dest_id", "tweet_id").toArray()

        println("Query 2 ok")
        // Save result to ArrayBuffer
        /*if (query.collect().length != 0) {
            collectionEdge += Edge(query.first().getString("user_send_local_id").toLong, query.first().getString("user_dest_id").toLong, query.first().getString("tweet_id").toString)
        }*/

        //collectionEdge.foreach(println(_))

        println("Query 2 Collect ok")

        // Convert vertices to RDD
        val VerticesRDD = sc.parallelize(collectionVertices)

        // Convert it to RDD
        val EdgeRDD = sc.parallelize(collectionEdge)

        println("Total vertices: " + collectionVertices.length)
        println("Total edges: " + collectionEdge.length)

        (VerticesRDD, EdgeRDD)
    }
}