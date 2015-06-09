import CassandraUtils.CassandraUtils
import MllibUtils.MllibUtils
import KCoreDecomposition.KCoreDecomposition

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.PartitionStrategy._

// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


// Useful links
// http://ampcamp.berkeley.edu/big-data-mini-course/graph-analytics-with-graphx.html
// https://spark.apache.org/docs/latest/graphx-programming-guide.html

object GraphxTesting{

    val RED = "\033[1;30m"
    val ENDC = "\033[0m"

    def color(str: String, col: String): String = "%s%s%s".format(col, str, ENDC)

    def main(args: Array[String]) {

        println("\n\n**************************************************************")
        println("******************       GraphxTesting      ******************")
        println("**************************************************************\n")

        val cu = new CassandraUtils
        val mu = new MllibUtils
        val kd = new KCoreDecomposition

        // Display only warning and infos messages
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)

        // Not displaying infos messages
        //Logger.getLogger("org").setLevel(Level.OFF)
        //Logger.getLogger("akka").setLevel(Level.OFF)

        // Spark configuration
        val sparkConf = new SparkConf(true)
            .setMaster("local[2]")
            .setAppName("GraphxTesting")
            .set("spark.cassandra.connection.host", "127.0.0.1") // Link to Cassandra

        // Init SparkContext
        val sc = new SparkContext(sparkConf)

        // Create Vertices and Edges
        val(users, relationships, defaultUser) = initGraph(sc)

        // Build the initial Graph
        val graph = Graph(users, relationships, defaultUser).cache()

        /*
        println("\n--------------------------------------------------------------")
        println("Operations on tweets")
        println("--------------------------------------------------------------\n")

        // See who communicates with who
        time { displayAllCommunications(graph) }

        // Let's find user id
        val id = time { findUserIDByName(graph, "Michael") }
        println("ID for user Michael is : " + id.toString)

        // Find username with user ID
        val name = time { findUserNameByID(graph, 1) }
        println("Name for id 1 is : " + name.toString)

        // get tweet content with tweet ID
        var resultGetTweetContentFromID =  time { cu getTweetContentFromID(sc,"606461329357045760") }
        println(resultGetTweetContentFromID)

        // this one does not exist
        resultGetTweetContentFromID =  time { cu getTweetContentFromID(sc,"604230254979346433") }
        println(resultGetTweetContentFromID)

        // Get tweets from user
        val resultGetTweetsIDFromUser = time { cu getTweetsIDFromUser(sc,"209144549") }
        resultGetTweetsIDFromUser.foreach(println(_))

        // Count in and out degrees
        time { inAndOutDegrees(graph) }
        */
        /*
        println("\n--------------------------------------------------------------")
        println("Community detection")
        println("--------------------------------------------------------------\n")

        // Call ConnectedComponents
        time { cc(graph, users) }

        // Call StronglyConnectedComponents
        time { scc(graph, 1) }

        // Get triangle Count
        time { getTriangleCount(graph, users) }

        // Get PageRank
        time { getPageRank(graph, users) }

        // K-Core decomposition
        time { kd run(graph, users, 4, 2) }

        // LabelPropagation
        val graphLabelPropagation = time { LabelPropagation.run(graph, 4).cache() }

        println("VERTICES")
        graphLabelPropagation.vertices.collect.foreach(println(_))

        val labelVertices = graphLabelPropagation.vertices

        val displayVertices = users.join(labelVertices).map {
            case (id, (username, rank)) => (id, username, rank)
        }
        println("VERTICES NAMED")

        // Print the result descending
        println(displayVertices.collect().sortBy(_._3).reverse.mkString("\n"))
        println("EDGES")

        graphLabelPropagation.edges.collect.foreach(println(_))

        */
        /*println("\n--------------------------------------------------------------")
        println("Mllib")
        println("--------------------------------------------------------------\n")

        // LDA
        // 1. Get every tweets from the graph and store it in corpus
        // 2. Call LDA method
        val corpus = time { cu getTweetsContentFromEdge(sc, graph.edges, true) }
        corpus.foreach(println(_))

        val numTopics = 10
        val numIterations = 10
        val numWordsByTopics = 10
        val numStopwords  = 20
        time { mu getLDA(sc, corpus, numTopics, numIterations, numWordsByTopics, numStopwords, true) }
        */


        /*
        println("\n**************************************************************")
        println("                       FIRST EXAMPLE                          ")
        println("**************************************************************")


        println("\n--------------------------------------------------------------")
        println("First Step - K-Core Decomposition algorithm")
        println("--------------------------------------------------------------")

        // K-Core decomposition
        val graph_2 = time { kd getKCoreGraph(graph, users, 5, 5) }.cache()

        graph_2.edges.collect.foreach(println(_))
        graph_2.vertices.collect.foreach(println(_))

        println("\n--------------------------------------------------------------")
        println("Second Step - Connected Components algorithm")
        println("--------------------------------------------------------------")

        // Call ConnectedComponents
        time { cc(graph_2, graph_2.vertices) }

        println("\n--------------------------------------------------------------")
        println("Third Step - Get Tweets from Edges")
        println("--------------------------------------------------------------")

        val corpus = time { cu getTweetsContentFromEdge(sc, graph_2.edges, true) }
        corpus.foreach(println(_))

        println("\n--------------------------------------------------------------")
        println("Fourth Step - LDA Algorithm")
        println("--------------------------------------------------------------")

        val numTopics = 10
        val numIterations = 10
        val numWordsByTopics = 10
        val numStopwords  = 20
        time { mu getLDA(sc, corpus, numTopics, numIterations, numWordsByTopics, numStopwords, true) }*/



        println("\n**************************************************************")
        println("                       SECOND EXAMPLE                         ")
        println("**************************************************************")

        println("\n--------------------------------------------------------------")
        println("First Step - Split community : \n" +
            "\t     Connected Components algorithm to find different\n" +
            "\t     communities")
        println("--------------------------------------------------------------")

        time { cc(graph, graph.vertices) }

        val subGraphes = splitCommunity(graph, users, false)

        println("\n--------------------------------------------------------------")
        println("Second Step - Calculate LDA for every communities\n" +
            "\t 1. Get Tweets from Edges\n" +
            "\t 2. LDA Algorithm")
        println("--------------------------------------------------------------")
        var iComm = 1
        for (community <- subGraphes){
            println("--------------------------")
            println("Community : " + iComm)
            println("--------------------------")
            //community.edges.collect().foreach(println(_))
            community.vertices.collect().foreach(println(_))

            println("--------------------------")
            println("Get Tweets from Edges")
            println("--------------------------")
            val corpus = time { cu getTweetsContentFromEdge(sc, community.edges, false) }

            println("--------------------------")
            println("LDA Algorithm")
            println("--------------------------")
            val numTopics = 5
            val numIterations = 10
            val numWordsByTopics = 5
            val numStopwords  = 0
            time { mu getLDA(sc, corpus, numTopics, numIterations, numWordsByTopics, numStopwords, true) }

            iComm +=1
        }
    }

    /**
     * splitCommunity
     *
     * Find and split communities in graph
     *
     * @param Graph[String,String] $graph - Graph element
     * @param RDD[(VertexId, (String))] $users - Vertices
     * @param Boolean $displayResult - if true, display println
     * @return ArrayBuffer[Graph[String,String]] - Contains one graph per community
     *
     */
    def splitCommunity(graph:Graph[String,String],users:RDD[(VertexId, (String))], displayResult:Boolean): ArrayBuffer[Graph[String,String]] ={

        // Find the connected components
        val cc = graph.connectedComponents().vertices

        // Join the connected components with the usernames and id
        // The result is an RDD not a Graph
        val ccByUsername = users.join(cc).map {
            case (id, (username, cc)) => (id, username, cc)
        }

        // Print the result
        val lowerIDPerCommunity = ccByUsername.map{ case (id, username, cc) => cc }.distinct()

        // Result will be stored in an array
        var result = ArrayBuffer[Graph[String,String]]()

        for (id <- lowerIDPerCommunity.toArray){

            //println("\nCommunity ID : " + id)

            val subGraphVertices = ccByUsername.filter{_._3 == id}.map { case (id, username, cc) => (id, username)}

            //subGraphVertices.foreach(println(_))

            // Create a new graph
            // And remove missing vertices as well as the edges to connected to them
            var tempGraph = Graph(subGraphVertices, graph.edges).subgraph(vpred = (id, username) => username != null)

            result += tempGraph
        }

        // Display communities
        if(displayResult){
            println("\nCommunities found " + result.size)
            for (community <- result) {
                println("-----------------------")
                //community.edges.collect().foreach(println(_))
                community.vertices.collect().foreach(println(_))
            }
        }

        return result
    }

    /**
     * getTriangleCount
     *
     * Compute the number of triangles passing through each vertex.
     *
     * @param Graph[String,String] $graph - Graph element
     * @param RDD[(VertexId, (String))] $users - Vertices
     * @return Unit
     *
     * @see [[org.apache.spark.graphx.lib.TriangleCount$#run]]
     */
    def getTriangleCount(graph:Graph[String,String], users:RDD[(VertexId, (String))]): Unit ={

        println(color("\nCall getTriangleCount" , RED))

        // Sort edges ID srcID < dstID
        val edges = graph.edges.map { e =>
            if (e.srcId < e.dstId) {
                Edge(e.srcId, e.dstId, e.attr)
            }
            else {
                Edge(e.dstId, e.srcId, e.attr)
            }
        }

        // Temporary graph
        val newGraph = Graph(users, edges, "").cache()

        // Find the triangle count for each vertex
        // TriangleCount requires the graph to be partitioned
        val triCounts = newGraph.partitionBy(PartitionStrategy.RandomVertexCut).cache().triangleCount().vertices

        val triCountByUsername = users.join(triCounts).map {
            case (id, (username, rank)) => (id, username, rank)
        }

        println("Display triangle's sum for each user")
        triCountByUsername.foreach(println)

        println("\nTotal: " + triCountByUsername.map{ case (id, username, rank) => rank }.distinct().count() + "\n")
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

        println(color("\nCall getPageRank" , RED))

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
    def inAndOutDegrees(graph:Graph[String,String]): Unit ={

        println(color("\nCall inAndOutDegrees", RED))

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
        userGraph.vertices.foreach {
            case (id, u) => println(s"User $id is called ${u.name} and received ${u.inDeg} tweets and send ${u.outDeg}.")
        }
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
     * @constructor findUserByID
     *
     * find user ID with username
     *
     * @param Graph[String,String] $graph - Graph element
     * @param Int $userID - User id
     * @return String - if success : username | failure : "user not found"
     */
    def findUserNameByID (graph:Graph[String,String], userID:Int) : String = {
        println(color("\nCall : findUserNameWithID", RED))

        graph.vertices.filter{ case (id, name) => id == userID }.collect.foreach {
            (e: (org.apache.spark.graphx.VertexId, String)) => return e._2
        }
        "user not found"
    }

    /**
     * @constructor findUserIDByName
     *
     * find username with id
     *
     * @param Graph[String,String] $graph - Graph element
     * @param String $userName - Username
     * @return String - if success : id found | failure : "0"
     */
    def findUserIDByName(graph:Graph[String,String], userName:String) : String = {
        println(color("\nCall : findUserIDWithName", RED))

        graph.vertices.filter( _._2 == "Michael" ).collect.foreach {
            (e: (org.apache.spark.graphx.VertexId, String)) => return e._1.toString
        }
        "0"
    }

    /**
     * @constructor displayAllCommunications
     *
     * display all communications between users
     *
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
     * @constructor time
     *
     * timer for profiling block
     *
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
     * @constructor initGraph
     *
     * init data - construct graph and populate it
     *
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
            ))

        // Create an RDD for edges
        val relationships: RDD[Edge[String]] =
            sc.parallelize(Array(
                Edge(2732329846L, 132988448L, "606460188367974400"),
                Edge(2732329846L, 2941487254L, "606461336986386435"),
                Edge(2732329846L, 601389784L, "606461384767897600"),
                Edge(601389784L, 2732329846L, "606461128055488512"),
                Edge(2941487254L, 1192483885L, "606461287820722176"),
                Edge(2941487254L, 132988448L, "606461112033275905"),
                Edge(132988448L, 838147628L, "606461464958795777"),
                Edge(838147628L, 132988448L, "606460199306698753"),
                Edge(838147628L, 473822999L, "606461472349257728"),
                Edge(465776805L, 2941487254L, "606461463524352002"),
                Edge(465776805L, 601389784L, "606460012626591744"),
                Edge(465776805L, 2249679902L, "606460015893987328"),
                Edge(2249679902L, 465776805L, "606461276378636289"),
                Edge(2932436311L, 465776805L, "606460093828329472"),
                Edge(1192483885L, 2941487254L, "606461532248121344"),
                Edge(465776805L, 2941487254L, "606460150308859904"),
                Edge(601389784L, 2732329846L, "606461431526133760"),
                Edge(2932436311L, 465776805L, "606460103198273536"),
                Edge(2941487254L, 465776805L, "606460245792071682"),
                Edge(2941487254L, 1192483885L, "606461533720334336"),
                Edge(2941487254L, 1192483885L, "606461215712378880"),
                Edge(1192483885L, 2941487254L, "606461128160378881"),
                Edge(1192483885L, 2941487254L, "606460290683670528"),
                Edge(2732329846L, 132988448L, "606461106333347840"),
                Edge(2941487254L, 132988448L, "606460373747814400"),
                Edge(132988448L, 2941487254L, "606460278247727105"),
                Edge(132988448L, 2941487254L, "606461340119498753"),
                Edge(132988448L, 2941487254L, "606460270664425472"),
                Edge(601389784L, 2732329846L, "606461376081436672"),
                Edge(601389784L, 2732329846L, "606461120489095168"),
                Edge(2732329846L, 2941487254L, "606460080603709440"),
                Edge(2732329846L, 2941487254L, "606461382322614272"),
                Edge(2564641105L,1518391292L,"606460348888064001"),
                Edge(1518391292L,2564641105L,"606461173672722432")
            ))

        // Define a default user in case there are relationship with missing user
        val defaultUser = "John Doe"

        (users, relationships, defaultUser)
    }
}