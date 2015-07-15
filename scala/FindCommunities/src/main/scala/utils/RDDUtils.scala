package utils

import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

// To make some of the examples work we will also need RDD

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


class RDDUtils {

    val RED = "\033[1;30m"
    val ENDC = "\033[0m"

    /**
     * @constructor ArrayToVertices
     *
     *              Convert ArrayBuffer to RDD containing Vertices
     *
     * @param SparkContext - $sc - SparkContext
     * @param ArrayBuffer[(Long, (String))] - $collection - Contains vertices
     *
     * @return RDD[Edge[String]] - RDD of vertices
     */
    def ArrayToVertices(sc: SparkContext, collection: ArrayBuffer[(Long, (String))]): RDD[(VertexId, (String))] = {
        sc.parallelize(collection)
    }

    /**
     * @constructor ArrayToEdges
     *
     *              Convert ArrayBuffer to RDD containing Edges
     *
     * @param SparkContext - $sc - SparkContext
     * @param ArrayBuffer[Edge[String]] - $collection - Contains edges
     *
     * @return RDD[Edge[String]] - RDD of edges
     */
    def ArrayToEdges(sc: SparkContext, collection: ArrayBuffer[Edge[String]]): RDD[Edge[String]] = {
        sc.parallelize(collection)
    }

    /**
     * @constructor findUserByIDInGraph
     *
     *              find user ID with username
     *
     * @param Graph[String,String] $graph - Graph element
     * @param Int $userID - User id
     * @return String - if success : username | failure : "user not found"
     */
    def findUserNameByIDInGraph(graph: Graph[String, String], userID: Int): String = {
        println(color("\nCall : findUserNameWithID", RED))

        graph.vertices.filter { case (id, name) => id.toString equals userID.toString }.collect().foreach {
            (e: (org.apache.spark.graphx.VertexId, String)) => return e._2
        }
        "user not found"
    }

    /**
     * @constructor findUserIDByNameInGraph
     *
     *              find username with id
     *
     * @param Graph[String,String] $graph - Graph element
     * @param String $userName - Username
     * @return String - if success : id found | failure : "0"
     */
    def findUserIDByNameInGraph(graph: Graph[String, String], userName: String): String = {
        println(color("\nCall : findUserIDWithName", RED))

        graph.vertices.filter(_._2 == userName).collect().foreach {
            (e: (org.apache.spark.graphx.VertexId, String)) => return e._1.toString
        }
        "0"
    }

    def color(str: String, col: String): String = "%s%s%s".format(col, str, ENDC)

    /**
     * @constructor displayAllCommunications
     *
     *              display all communications between users
     *
     * @param Graph[String,String] $graph - Graph element
     * @return Unit
     */
    def displayAllCommunications(graph: Graph[String, String]): Unit = {

        println(color("\nCall : displayAllCommunications", RED))
        println("Users communications: ")

        val facts: RDD[String] = graph.triplets.map(triplet => triplet.srcAttr + " communicate with " +
            triplet.dstAttr + " with tweet id " + triplet.attr)

        facts.collect().foreach(println(_))
    }
}