import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

// Useful links
// https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
// http://planetcassandra.org/getting-started-with-apache-spark-and-cassandra/
// https://bcomposes.wordpress.com/2013/02/09/using-twitter4j-with-scala-to-access-streaming-tweets/
// https://github.com/datastax/spark-cassandra-connector/blob/master/doc/5_saving.md

object GraphxTesting{
    
    def main(args: Array[String]) {
        
        // Display only warning and infos messages
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
        
        // Not displaying infos messages
        //Logger.getLogger("org").setLevel(Level.OFF)
        //Logger.getLogger("akka").setLevel(Level.OFF)
        
        // Spark configuration
        val sparkConf = new SparkConf(true)
        .setMaster("local[4]")
        .setAppName("GraphxTesting")
        .set("spark.cassandra.connection.host", "127.0.0.1") // Link to Cassandra

        val sc = new SparkContext(sparkConf)

        // Create Vertices and Edges
        val(users, relationships, defaultUser) = initGraph(sc)
        
        // Build the initial Graph
        val graph = Graph(users, relationships, defaultUser)

        // See who communicates with who
        displayAllCommunications(graph)

        // Let's find user id
        val id = findUserIDWithName(graph, "Michael")
        println("ID for user Michael : " + id.toString)

        // Find username with user ID
        val name = findUserNameWithID(graph, 1)
        println("Name for id 1: " + name.toString)


        // Create User class
        case class User(name: String, // Username
                        inDeg: Int,   // Received tweets
                        outDeg: Int)  // Sent tweets

        // Create user Graph
        // def mapVertices[VD2](map: (VertexID, VD) => VD2): Graph[VD2, ED]
        val initialUserGraph: Graph[User, String] = graph.mapVertices {
            case (id, (name)) => User(name, 0, 0)
        }

        initialUserGraph.edges.collect.foreach(println(_))


        // Fill in the degree informations (out and in degrees)
        val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
            case (id, u, inDegOpt) => User(u.name, inDegOpt.getOrElse(0), u.outDeg)
        }.outerJoinVertices(initialUserGraph.outDegrees) {
            case (id, u, outDegOpt) => User(u.name, u.inDeg, outDegOpt.getOrElse(0))
        }
        
        // Display the userGraph    
        println("\n\nUsers out and in degrees: ")
        userGraph.vertices.foreach { 
            case (id, u) => println(s"User $id is called ${u.name} and received ${u.inDeg} tweets and send ${u.outDeg}.")
        }
        
        println(graph.numEdges)

        /**
         * StronglyConnectedComponents
         *
         * Compute the strongly connected component (SCC) of each vertex and return a graph with the
         * vertex value containing the lowest vertex id in the SCC containing that vertex.
         */

        val sccGraph = graph.stronglyConnectedComponents(5)
        users.join(sccGraph.vertices).foreach(println)

        val nameOf = relationships.collect.foreach (println)


        sccGraph.vertices.map {
          case (member, leader) => s"$member is in the group of $leader's edge"
        }.collect.foreach(println)

        //println(sccGraph)
        /*sccGraph.edges.collect.foreach(println(_))
        sccGraph.edges.count

        sccGraph.vertices.collect.foreach(println(_))
        sccGraph.vertices.count*/


        /**
         * ConnectedComponents
         *
         * Compute the connected component membership of each vertex and return a graph with the vertex
         * value containing the lowest vertex id in the connected component containing that vertex.
         *
         * @see [[org.apache.spark.graphx.lib.ConnectedComponents$#run]]
         */



        /**
         * TriangleCount
         *
         * Compute the number of triangles passing through each vertex.
         *
         * @see [[org.apache.spark.graphx.lib.TriangleCount$#run]]
         */


        /**
         * PageRank
         *
         * Run PageRank for a fixed number of iterations returning a graph with vertex attributes
         * containing the PageRank and edge attributes the normalized edge weight.
         *
         * @see [[org.apache.spark.graphx.lib.PageRank$#run]]
         */
    }


    /**
     * @constructor init data - construct graph and populate it
     * @param SparkContext $sc - Sparkcontext
     * @return RDD[(VertexId, (String))] - users (Vertices)
     *         RDD[Edge[String]] - relationship (Edges)
     *         String - default user
     */
    def initGraph(sc:SparkContext): (RDD[(VertexId, (String))], RDD[Edge[String]], String) ={

      println("Call : initGraph\n")

      // Create an RDD for the vertices
      val users: RDD[(VertexId, (String))] =
        sc.parallelize(Array((1L, "Michael"),
          (2L, "David"),
          (3L, "Sarah"),
          (4L, "Jean"),
          (5L, "Raphael"),
          (6L, "Lucie"),
          (7L, "Harold"),
          (8L, "Pierre"),
          (9L, "Christophe"),
          (10L, "Zoe")
        ))

      // Create an RDD for edges
      val relationships: RDD[Edge[String]] =
        sc.parallelize(Array(Edge(1L, 2L, "1"),
          Edge(1L, 7L, "2"),
          Edge(1L, 6L, "3"),
          Edge(6L, 1L, "4"),
          Edge(7L, 8L, "5"),
          Edge(7L, 2L, "6"),
          Edge(2L, 10L, "7"),
          Edge(10L, 2L, "8"),
          Edge(10L, 3L, "9"),
          Edge(9L, 7L, "10"),
          Edge(9L, 6L, "11"),
          Edge(9L, 5L, "12"),
          Edge(5L, 9L, "13"),
          Edge(4L, 9L, "14"),
          Edge(8L, 7L, "15"),
          Edge(9L, 7L, "16"),
          Edge(6L, 1L, "17"),
          Edge(4L, 9L, "18"),
          Edge(7L, 9L, "19"),
          Edge(7L, 8L, "20"),
          Edge(7L, 8L, "21"),
          Edge(8L, 7L, "22"),
          Edge(8L, 7L, "23"),
          Edge(1L, 2L, "24"),
          Edge(7L, 2L, "25"),
          Edge(2L, 7L, "26"),
          Edge(2L, 7L, "27"),
          Edge(2L, 7L, "28"),
          Edge(6L, 1L, "29"),
          Edge(6L, 1L, "30"),
          Edge(1L, 7L, "31"),
          Edge(1L, 7L, "32")
        ))

      // Define a default user in case there are relationship with missing user
      val defaultUser = "John Doe"

      (users, relationships, defaultUser)
    }

    /**
    * @constructor find user ID with username
    * @param Graph[String,String] $graph - Graph element
    * @param Int $userID - User id
    * @return String - if success : username | failure : "user not found"
    */
    def findUserNameWithID (graph:Graph[String,String], userID:Int) : String = {
      println("Call : findUserNameWithID")

      graph.vertices.filter{ case (id, name) => id == userID }.collect.foreach {
            (e: (org.apache.spark.graphx.VertexId, String)) => return e._2
        }
        "user not found"
    }

    /**
    * @constructor find username with id
    * @param Graph[String,String] $graph - Graph element
    * @param String $userName - Username
    * @return String - if success : id found | failure : "0"
    */
    def findUserIDWithName(graph:Graph[String,String], userName:String) : String = {
      println("Call : findUserIDWithName")

      graph.vertices.filter( _._2 == "Michael" ).collect.foreach {
            (e: (org.apache.spark.graphx.VertexId, String)) => return e._1.toString
        }
        "0"
    }

    /**
     * @constructor display all communications between users
     * @param Graph[String,String] $graph - Graph element
     * @return Unit
     */
    def displayAllCommunications(graph:Graph[String,String]): Unit ={

      println("Call : displayAllCommunications")
      println("Users communications: ")

      val facts: RDD[String] = graph.triplets.map(triplet =>  triplet.srcAttr + " communicate with " +
        triplet.dstAttr + " with tweet id " + triplet.attr)

      facts.collect.foreach(println(_))
    }
}