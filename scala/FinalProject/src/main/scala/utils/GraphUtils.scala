package utils

// To make some of the examples work we will also need RDD

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


class GraphUtils {

    val RED = "\033[1;30m"
    val ENDC = "\033[0m"
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

    /**
     * @constructor getPageRank
     *
     *              Run PageRank for a fixed number of iterations returning a graph with vertex attributes
     *              containing the PageRank and edge attributes the normalized edge weight.
     *
     * @param Graph[String,String] $graph - Graph element
     * @param RDD[(VertexId, (String))] $users - Vertices
     * @return Unit
     *
     * @see [[org.apache.spark.graphx.lib.PageRank$#run]]
     */
    def getPageRank(graph: Graph[String, String], users: RDD[(VertexId, (String))]): Unit = {

        println(color("\nCall getPageRank", RED))

        val ranks = graph.pageRank(0.00001).vertices

        val ranksByUsername = users.join(ranks).map {
            case (id, (username, rank)) => (id, username, rank)
        }

        // Print the result descending
        println(ranksByUsername.collect().sortBy(_._3).reverse.mkString("\n"))
    }

    /**
     * @constructor inAndOutDegrees
     *
     * @param Graph[String,String] $graph - Graph element
     * @return Unit
     *
     */
    def inAndOutDegrees(graph: Graph[String, String]): Unit = {

        println(color("\nCall inAndOutDegrees", RED))

        // Create User class
        case class User(name: String, // Username
                        inDeg: Int, // Received tweets
                        outDeg: Int) // Sent tweets

        // Create user Graph
        // def mapVertices[VD2](map: (VertexID, VD) => VD2): Graph[VD2, ED]
        val initialUserGraph: Graph[User, String] = graph.mapVertices {
            case (id, (name)) => User(name, 0, 0)
        }

        //initialUserGraph.edges.collect.foreach(println(_))


        // Fill in the degree informations (out and in degrees)
        val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
            case (id, u, inDegOpt) => User(u.name, inDegOpt.getOrElse(0), u.outDeg)
        }.outerJoinVertices(initialUserGraph.outDegrees) {
            case (id, u, outDegOpt) => User(u.name, u.inDeg, outDegOpt.getOrElse(0))
        }

        // Display the userGraph
        userGraph.vertices.foreach {
            case (id, u) => println(s"User $id is called ${u.name} and received ${u.inDeg} tweets and send ${u.outDeg}.")
        }
    }

    def color(str: String, col: String): String = "%s%s%s".format(col, str, ENDC)
}