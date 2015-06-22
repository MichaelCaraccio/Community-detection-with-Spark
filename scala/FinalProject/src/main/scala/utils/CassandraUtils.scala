package utils

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{DataFrame, Strategy, SQLContext, SchemaRDD}

// Enable Cassandra-specific functions on the StreamingContext, DStream and RDD:

import com.datastax.spark.connector._

// To make some of the examples work we will also need RDD

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraSQLContext

@SerialVersionUID(100L)
class CassandraUtils extends Serializable{

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

    def color(str: String, col: String): String = "%s%s%s".format(col, str, ENDC)

    // (RDD[(VertexId, (String))], RDD[Edge[String]])
    def getAllCommunications(sc: SparkContext): Graph[String, String] = {
        println(color("\nCall getAllCommunications", RED))


        /* val users: RDD[(VertexId, (String))] =
             sc.parallelize(List(
                 (2732329846L, "Michael"),
                 (132988448L, "David"),
                 (473822999L, "Sarah"),
                 (2932436311L, "Jean"),
                 (2249679902L, "Raphael"),
                 (601389784L, "Lucie"),
                 (2941487254L, "Harold"),
                 (1192483885L, "Pierre"),
                 (465776805L, "Christophe"),
                 (838147628L, "Zoe"),
                 (2564641105L, "Fabien"),
                 (1518391292L, "Nicolas")
             ))*/


        // Collection of vertices (contains users)
       // val collectionVertices = ListBuffer[(Long, String)]()


       // val users: RDD[(VertexId, (String))] = sc.parallelize(collectionVertices)


        //val con = sc.cassandraTable("twitter", "user_filtered")
        //con.toArray.foreach(println)
        /*println("Test -1")

        var t0 = System.nanoTime()
            for (row <- query) {

            }

        var t1 = System.nanoTime()
        println("Elapsed time: " + (t1 - t0) + "ns")*/

       // val query = sc.cassandraTable("twitter", "user_filtered").select("user_local_id", "user_screen_name")


        /*val con = query.map{
             case result => (result._1, result._2)
         }*/
        val cc = new CassandraSQLContext(sc)

        println("Test 0")
        var t0 = System.nanoTime()
        val rdd0 = cc.sql("SELECT user_local_id, user_screen_name from twitter.user_filtered")

        val pelo = rdd0.map(p => (p(0).toString.toLong, p(1).toString)).cache()

        val rdd1 = cc.sql("SELECT tweet_id, user_send_local_id, user_dest_id from twitter.users_communicate")

        val pelo2 = rdd1.map(p => Edge(p(1).toString.toLong, p(2).toString.toLong, p(0).toString)).cache()

        println("wesh")

        Graph(pelo, pelo2)

        /*println("okkk")

        graphh.vertices.foreach(println(_))


        //pelo.foreach(println(_))

        println("After collecting")

        rdd0.show()

        for (row <- rdd0) {
            //println(row(0))

            collectionVertices += ((row(0).toString.toLong, row(1).toString))
            //collectionVertices.append((row(0).toString.toLong, row(1).toString))
        }
        var t1 = System.nanoTime()
        println("Elapsed time: " + (t1 - t0) + "ns")


        println("Test 1")
        t0 = System.nanoTime()

        val rdd = cc.sql("SELECT user_local_id, user_screen_name from twitter.user_filtered LIMIT 100").persist()
        for (row <- rdd) {
            collectionVertices += ((row(0).toString.toLong, row(1).toString))
        }
        rdd.unpersist()
        t1 = System.nanoTime()
        println("Elapsed time: " + (t1 - t0) + "ns")



        println("Test 2")
        t0 = System.nanoTime()
        val rdd2 = cc.sql("SELECT user_local_id, user_screen_name from twitter.user_filtered limit 10000").cache()
        for (row <- rdd2) {
            collectionVertices += ((row(0).toString.toLong, row(1).toString))
        }
        t1 = System.nanoTime()
        println("Elapsed time: " + (t1 - t0) + "ns")

        println("Test 3")
        t0 = System.nanoTime()

        for (row <- cc.sql("SELECT user_local_id, user_screen_name from twitter.user_filtered limit 10000")) {
            collectionVertices += ((row(0).toString.toLong, row(1).toString))
        }
        t1 = System.nanoTime()
        println("Elapsed time: " + (t1 - t0) + "ns")






        println("f")
        // println(rdd.take(1))
        println("f2")
*
        /*
        println("Query 1 ok")
         */
        // Save result to ArrayBuffer
        //if (query.collect().length != 0) {
            //collectionVertices += ((query.first().getString("user_local_id").toLong, query.first().getString("user_local_id").toString))
            println(query.first().getString("user_local_id"))
       // }

        //collectionVertices.foreach(println(_))

        println("Query 1 Collect ok")



        // Collection of edges (contains communications between users)
        val collectionEdge = ArrayBuffer[Edge[String]]()


        //query = sc.cassandraTable("twitter", "users_communicate").select("user_send_local_id", "user_dest_id", "tweet_id").toArray()

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

        (VerticesRDD, EdgeRDD)*/
    }
}