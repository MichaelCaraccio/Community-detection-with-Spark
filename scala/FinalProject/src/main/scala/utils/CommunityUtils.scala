package utils

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.math._
import scala.reflect.ClassTag
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import org.apache.spark.graphx.PartitionStrategy._

class CommunityUtils extends Logging with Serializable {

    val RED = "\033[1;30m"
    val ENDC = "\033[0m"

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
    def splitCommunity(graph: Graph[String, String], users: RDD[(VertexId, (String))], displayResult: Boolean): ArrayBuffer[Graph[String, String]] = {

        println(color("\nCall SplitCommunity", RED))

        val graph_2 = getKCoreGraph(graph, users, 4, false).cache()

        // Find the connected components
        val cc = graph_2.connectedComponents().vertices

        // Join the connected components with the usernames and id
        // The result is an RDD not a Graph
        val ccByUsername = users.join(cc).map {
            case (id, (username, cc)) => (id, username, cc)
        }

        // Print the result
        val lowerIDPerCommunity = ccByUsername.map { case (id, username, cc) => cc }.distinct()

        // Result will be stored in an array
        var result = ArrayBuffer[Graph[String, String]]()
        println("--------------------------")
        println("Total community found: " + lowerIDPerCommunity.collect().length)
        println("--------------------------")
        for (id <- lowerIDPerCommunity.collect()) {

            println("\nCommunity ID : " + id)

            val subGraphVertices = ccByUsername.filter {
                _._3 == id
            }.map { case (id, username, cc) => (id, username) }

            //subGraphVertices.foreach(println(_))

            // Create a new graph
            // And remove missing vertices as well as the edges to connected to them
            var tempGraph = Graph(subGraphVertices, graph_2.edges).subgraph(vpred = (id, username) => username != null)

            result += tempGraph
        }

        // Display communities
        if (displayResult) {
            println("\nCommunities found " + result.size)
            for (community <- result) {
                println("-----------------------")
                //community.edges.collect().foreach(println(_))
                community.vertices.collect().foreach(println(_))
            }
        }

        result
    }

    /**
     * Compute the k-core decomposition of the graph for all k <= kmax. This
     * uses the iterative pruning algorithm discussed by Alvarez-Hamelin et al.
     * in K-Core Decomposition: a Tool For the Visualization of Large Scale Networks
     * (see <a href="http://arxiv.org/abs/cs/0504107">http://arxiv.org/abs/cs/0504107</a>).
     *
     * @tparam VD the vertex attribute type (discarded in the computation)
     * @tparam ED the edge attribute type (preserved in the computation)
     *
     * @param graph the graph for which to compute the connected components
     * @param kmax the maximum value of k to decompose the graph
     *
     * @return a graph where the vertex attribute is the minimum of
     *         kmax or the highest value k for which that vertex was a member of
     *         the k-core.
     *
     * @note This method has the advantage of returning not just a single kcore of the
     *       graph but will yield all the cores for k > kmin.
     */
    def getKCoreGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                  users: RDD[(VertexId, (String))],
                                                  kmin: Int,
                                                  displayResult: Boolean): Graph[String, ED] = {

        // Graph[(Int, Boolean), ED] - boolean indicates whether it is active or not
        var g = graph.outerJoinVertices(graph.degrees)((vid, oldData, newData) => newData.getOrElse(0)).cache()
        val degrees = graph.degrees

        println(color("\nCall KCoreDecomposition", RED))


        g = computeCurrentKCore(g, kmin).cache()
        val testK = kmin
        val vCount = g.vertices.filter { case (vid, vd) => vd >= kmin }.count()
        val eCount = g.triplets.map { t => t.srcAttr >= testK && t.dstAttr >= testK }.count()
        val v = g.vertices.filter { case (vid, vd) => vd >= kmin }

        // Display informations
        if (displayResult) {

            val numVertices = degrees.count()

            logWarning(s"Number of vertices: $numVertices")
            logWarning(s"Degree sample: ${degrees.take(10).mkString(", ")}")
            logWarning(s"Degree distribution: " + degrees.map { case (vid, data) => (data, 1) }.reduceByKey(_ + _).collect().mkString(", "))
            logWarning(s"Degree distribution: " + degrees.map { case (vid, data) => (data, 1) }.reduceByKey(_ + _).take(10).mkString(", "))
            logWarning(s"K=$kmin, V=$vCount, E=$eCount")
        }

        // Create new RDD users
        val newUser = users.join(v).map {
            case (id, (username, rank)) => (id, username)
        }

        // Create a new graph
        val gra = Graph(newUser, g.edges)

        // Remove missing vertices as well as the edges to connected to them
        gra.subgraph(vpred = (id, username) => username != null)
    }

    def computeCurrentKCore[ED: ClassTag](graph: Graph[Int, ED], k: Int) = {
        println("Computing kcore for k="+k)
        def sendMsg(et: EdgeTriplet[Int, ED]): Iterator[(VertexId, Int)] = {
            if (et.srcAttr < 0 || et.dstAttr < 0) {
                // if either vertex has already been turned off we do nothing
                Iterator.empty
            } else if (et.srcAttr < k && et.dstAttr < k) {
                // tell both vertices to turn off but don't need change count value
                Iterator((et.srcId, -1), (et.dstId, -1))

            } else if (et.srcAttr < k) {
                // if src is being pruned, tell dst to subtract from vertex count
                Iterator((et.srcId, -1), (et.dstId, 1))

            } else if (et.dstAttr < k) {
                // if dst is being pruned, tell src to subtract from vertex count
                Iterator((et.dstId, -1), (et.srcId, 1))

            } else {
                Iterator.empty
            }
        }

        // subtracts removed neighbors from neighbor count and tells vertex whether it was turned off or not
        def mergeMsg(m1: Int, m2: Int): Int = {
            if (m1 < 0 || m2 < 0) {
                -1
            } else {
                m1 + m2
            }
        }

        def vProg(vid: VertexId, data: Int, update: Int): Int = {
            if (update < 0) {
                // if the vertex has turned off, keep it turned off
                -1
            } else {
                // subtract the number of neighbors that have turned off this round from
                // the count of active vertices
                // TODO(crankshaw) can we ever have the case data < update?
                max(data - update, 0)
            }
        }

        println("avant pregel")
        // Note that initial message should have no effect
        Pregel(graph, 0)(vProg, sendMsg, mergeMsg)
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
    def getTriangleCount(graph: Graph[String, String], users: RDD[(VertexId, (String))]): Unit = {

        println(color("\nCall getTriangleCount", RED))

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

        println("\nTotal: " + triCountByUsername.map { case (id, username, rank) => rank }.distinct().count() + "\n")
    }

    /**
     * @constructor ConnectedComponents
     *
     *              Compute the connected component membership of each vertex and return a graph with the vertex
     *              value containing the lowest vertex id in the connected component containing that vertex.
     *
     * @param Graph[String,String] $graph - Graph element
     * @param RDD[(VertexId, (String))] $users - Vertices
     * @return Unit
     *
     * @see [[org.apache.spark.graphx.lib.ConnectedComponents$#run]]
     */
    def cc(graph: Graph[String, String], users: RDD[(VertexId, (String))]): Unit = {
        println(color("\nCall ConnectedComponents", RED))

        // Find the connected components
        val cc = graph.connectedComponents().vertices

        // Join the connected components with the usernames and id
        val ccByUsername = users.join(cc).map {
            case (id, (username, cc)) => (id, username, cc)
        }
        // Print the result
        println(ccByUsername.collect().sortBy(_._3).mkString("\n"))

        println("\nTotal groups: " + ccByUsername.map { case (id, username, cc) => cc }.distinct().count() + "\n")
    }

    def color(str: String, col: String): String = "%s%s%s".format(col, str, ENDC)

    /**
     * @constructor StronglyConnectedComponents
     *
     *              Compute the strongly connected component (SCC) of each vertex and return a graph with the
     *              vertex value containing the lowest vertex id in the SCC containing that vertex.
     *
     *              Display edges's membership and total groups
     *
     * @param Graph[String,String] $graph - Graph element
     * @param Int $iteration - Number of iteration
     * @return Unit
     */
    def scc(graph: Graph[String, String], iteration: Int): Unit = {

        println(color("\nCall StronglyConnectedComponents : iteration : " + iteration, RED))
        val sccGraph = graph.stronglyConnectedComponents(5)

        val connectedGraph = sccGraph.vertices.map {
            case (member, leaderGroup) => s"$member is in the group of $leaderGroup's edge"
        }

        val totalGroups = sccGraph.vertices.map {
            case (member, leaderGroup) => leaderGroup
        }

        connectedGraph.collect().foreach(println)

        println("\nTotal groups: " + totalGroups.distinct().count() + "\n")
    }
}