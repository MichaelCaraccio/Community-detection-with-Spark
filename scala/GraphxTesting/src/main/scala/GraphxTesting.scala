import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

// Useful links
// http://ampcamp.berkeley.edu/big-data-mini-course/graph-analytics-with-graphx.html
// https://spark.apache.org/docs/latest/graphx-programming-guide.html

object GraphxTesting{

    val RED = "\033[1;30m"
    val ENDC = "\033[0m"

    def color(str: String, col: String): String = "%s%s%s".format(col, str, ENDC)

    def main(args: Array[String]) {

        println("\n\n***************************************************")
        println("************       GraphxTesting      *************")
        println("***************************************************\n")



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
            //.set("spark.cassandra.connection.host", "127.0.0.1") // Link to Cassandra

        // Init SparkContext
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

        //initialUserGraph.edges.collect.foreach(println(_))


        // Fill in the degree informations (out and in degrees)
        val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
            case (id, u, inDegOpt) => User(u.name, inDegOpt.getOrElse(0), u.outDeg)
        }.outerJoinVertices(initialUserGraph.outDegrees) {
            case (id, u, outDegOpt) => User(u.name, u.inDeg, outDegOpt.getOrElse(0))
        }

        // Display the userGraph    
        println(color("\nUsers out and in degrees: ", RED))
        userGraph.vertices.foreach {
            case (id, u) => println(s"User $id is called ${u.name} and received ${u.inDeg} tweets and send ${u.outDeg}.")
        }

        // Call ConnectedComponents
        time { cc(graph, users) }

        // Call StronglyConnectedComponents
        time { scc(graph, 1) }

        // Get PageRank
        time { getPageRank(graph, users) }


        //println(sccGraph)
        /*sccGraph.edges.collect.foreach(println(_))
        sccGraph.edges.count

        sccGraph.vertices.collect.foreach(println(_))
        sccGraph.vertices.count*/




        /**
         * TriangleCount
         *
         * Compute the number of triangles passing through each vertex.
         *
         * @see [[org.apache.spark.graphx.lib.TriangleCount$#run]]
         */


    }


    /**
     * @constructor getPageRank
     *
     * Run PageRank for a fixed number of iterations returning a graph with vertex attributes
     * containing the PageRank and edge attributes the normalized edge weight.
     *
     * @param Graph[String,String] $graph - Graph element
     * @param RDD[(VertexId, (String))] $users - Vertices
     * @return Unit
     *
     * @see [[org.apache.spark.graphx.lib.PageRank$#run]]
     */
    def getPageRank(graph:Graph[String,String], users:RDD[(VertexId, (String))]): Unit ={

        println(color("\nCall PageRank" , RED))

        val ranks = graph.pageRank(0.00001).vertices

        val ranksByUsername = users.join(ranks).map {
            case (id, (username, rank)) => (id, username, rank)
        }

        // Print the result descending
        println(ranksByUsername.collect().sortBy(_._3).reverse.mkString("\n"))
    }

    /**
     * @constructor ConnectedComponents
     *
     * Compute the connected component membership of each vertex and return a graph with the vertex
     * value containing the lowest vertex id in the connected component containing that vertex.
     *
     * @param Graph[String,String] $graph - Graph element
     * @param RDD[(VertexId, (String))] $users - Vertices
     * @return Unit
     *
     * @see [[org.apache.spark.graphx.lib.ConnectedComponents$#run]]
     */
    def cc(graph:Graph[String,String], users:RDD[(VertexId, (String))]): Unit ={
        println(color("\nCall ConnectedComponents" , RED))

        // Find the connected components
        val cc = graph.connectedComponents().vertices

        // Join the connected components with the usernames and id
        val ccByUsername = users.join(cc).map {
            case (id, (username, cc)) => (id, username, cc)
        }
        // Print the result
        println(ccByUsername.collect().sortBy(_._3).mkString("\n"))

        println("\nTotal groups: " + ccByUsername.map{ case (id, username, cc) => cc }.distinct().count() + "\n")

    }


    /**
     * @constructor StronglyConnectedComponents
     *
     * Compute the strongly connected component (SCC) of each vertex and return a graph with the
     * vertex value containing the lowest vertex id in the SCC containing that vertex.
     *
     * Display edges's membership and total groups
     *
     * @param Graph[String,String] $graph - Graph element
     * @param Int $iteration - Number of iteration
     * @return Unit
     */
    def scc(graph:Graph[String,String], iteration:Int): Unit ={

        println(color("\nCall StronglyConnectedComponents : iteration : " + iteration , RED))
        val sccGraph = graph.stronglyConnectedComponents(5)

        val connectedGraph = sccGraph.vertices.map {
            case (member, leaderGroup) => s"$member is in the group of $leaderGroup's edge"
        }

        val totalGroups = sccGraph.vertices.map {
            case (member, leaderGroup) => leaderGroup
        }

        connectedGraph.collect.foreach(println)

        println("\nTotal groups: " + totalGroups.distinct().count() + "\n")
    }

    /**
     * @constructor find user ID with username
     * @param Graph[String,String] $graph - Graph element
     * @param Int $userID - User id
     * @return String - if success : username | failure : "user not found"
     */
    def findUserNameWithID (graph:Graph[String,String], userID:Int) : String = {
        println(color("\nCall : findUserNameWithID", RED))

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
        println(color("\nCall : findUserIDWithName", RED))

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

        println(color("\nCall : displayAllCommunications", RED))
        println("Users communications: ")

        val facts: RDD[String] = graph.triplets.map(triplet =>  triplet.srcAttr + " communicate with " +
            triplet.dstAttr + " with tweet id " + triplet.attr)

        facts.collect.foreach(println(_))
    }

    /**
     * @constructor timer for profiling block
     * @param R $block - Block executed
     * @return Unit
     */
    def time[R](block: => R): R = {
        val t0 = System.nanoTime()
        val result = block    // call-by-name
        val t1 = System.nanoTime()
        println("Elapsed time: " + (t1 - t0) / 1000000000.0 + " seconds")
        result
    }

    /**
     * @constructor init data - construct graph and populate it
     * @param SparkContext $sc - Sparkcontext
     * @return RDD[(VertexId, (String))] - users (Vertices)
     *         RDD[Edge[String]] - relationship (Edges)
     *         String - default user
     */
    def initGraph(sc:SparkContext): (RDD[(VertexId, (String))], RDD[Edge[String]], String) ={
        println(color("\nCall : initGraph", RED))

        // Create an RDD for the vertices
        val users: RDD[(VertexId, (String))] =
            sc.parallelize(Array(
                (1L, "Michael"),
                (2L, "David"),
                (3L, "Sarah"),
                (4L, "Jean"),
                (5L, "Raphael"),
                (6L, "Lucie"),
                (7L, "Harold"),
                (8L, "Pierre"),
                (9L, "Christophe"),
                (10L, "Zoe"),
                (11L, "Fabien"),
                (12L, "Nicolas")
            ))

        // Create an RDD for edges
        val relationships: RDD[Edge[String]] =
            sc.parallelize(Array(
                Edge(1L, 2L, "1"),
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
                Edge(1L, 7L, "32"),
                Edge(11L,12L,"33"),
                Edge(12L,11L,"34")
            ))

        // Define a default user in case there are relationship with missing user
        val defaultUser = "John Doe"

        (users, relationships, defaultUser)
    }

}