import CassandraUtils.CassandraUtils
import MllibUtils.MllibUtils
import CommunityUtils.CommunityUtils
import GraphUtils.GraphUtils
import RDDUtils.RDDUtils

import scala.collection.mutable.ArrayBuffer
import scala.math._

import org.apache.spark.mllib.linalg.{Vector, DenseMatrix, Matrix, Vectors}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering._


import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.twitter
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.twitter.TwitterUtils._
import org.apache.spark.streaming._

// Cassandra

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

import scala.util.matching.Regex


// To make some of the examples work we will also need RDD

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


// Useful links
// http://ampcamp.berkeley.edu/big-data-mini-course/graph-analytics-with-graphx.html
// https://spark.apache.org/docs/latest/graphx-programming-guide.html

object FinalProject {

    // Terminal color
    val RED = "\033[1;30m"
    val ENDC = "\033[0m"

    def color(str: String, col: String): String = "%s%s%s".format(col, str, ENDC)

    // Seed for murmurhash
    private val defaultSeed = 0xadc83b19L

    def main(args: Array[String]) {

        println("\n\n**************************************************************")
        println("******************       FinalProject      ******************")
        println("**************************************************************\n")

        val cu = new CassandraUtils
        val comUtils = new CommunityUtils
        val gu = new GraphUtils
        val ru = new RDDUtils

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

        // Create Vertices and Edges
        //val(users, relationships, defaultUser) = initGraph(sc)

        // Build the initial Graph
        //val graph = Graph(users, relationships, defaultUser).cache()


        // Filters by words that contains @
        val words = Array(" @")

        // Pattern used to find users
        val pattern = new Regex("\\@\\w{3,}")
        val patternURL = new Regex("(http|ftp|https)://[A-Za-z0-9-_]+.[A-Za-z0-9-_:%&?/.=]+")
        val patternSmiley = new Regex("((?::|;|=)(?:-)?(?:\\)|D|P|3|O))")

        // First twitter instance : Used for stream
        /*val twitterstream = new TwitterFactory().getInstance()
        twitterstream.setOAuthConsumer("MCrQfOAttGZnIIkrqZ4lQA9gr", "5NnYhhGdfyqOE4pIXXdYkploCybQMzFJiQejZssK4a3mNdkCoa")
        twitterstream.setOAuthAccessToken(new AccessToken("237197078-6zwzHsuB3VY3psD5873hhU3KQ1lSVQlOXyBhDqpG", "UIMZ1aD06DObpKI741zC8wHZF8jkj1bh02Lqfl5cQ76Pl"))
        */
        System.setProperty("twitter4j.http.retryCount", "3")
        System.setProperty("twitter4j.http.retryIntervalSecs", "10")
        System.setProperty("twitter4j.async.numThreads", "10")

        // Set the system properties so that Twitter4j library used by twitter stream
        // can use them to generat OAuth credentials
        System.setProperty("twitter4j.oauth.consumerKey", "MCrQfOAttGZnIIkrqZ4lQA9gr")
        System.setProperty("twitter4j.oauth.consumerSecret", "5NnYhhGdfyqOE4pIXXdYkploCybQMzFJiQejZssK4a3mNdkCoa")
        System.setProperty("twitter4j.oauth.accessToken", "237197078-6zwzHsuB3VY3psD5873hhU3KQ1lSVQlOXyBhDqpG")
        System.setProperty("twitter4j.oauth.accessTokenSecret", "UIMZ1aD06DObpKI741zC8wHZF8jkj1bh02Lqfl5cQ76Pl")

        val ssc = new StreamingContext(sparkConf, Seconds(10))
        val stream = TwitterUtils.createStream(ssc, None, words)

        // Init SparkContext
        val sc = ssc.sparkContext


        // Stream about users
        val usersStream = stream.map { status => (
            status.getUser.getId.toString,
            abs(murmurHash64A(status.getUser.getScreenName.getBytes)),
            status.getUser.getName.toString,
            status.getUser.getLang,
            status.getUser.getFollowersCount.toString,
            status.getUser.getFriendsCount.toString,
            status.getUser.getScreenName,
            status.getUser.getStatusesCount.toString)
        }


        // Stream about communication between two users
        val commStream = stream.map { status => (
            status.getId.toString, //tweet_id
            status.getUser.getId.toString, // user_send_twitter_ID
            status.getUser.getScreenName, // user_send_name
            if (pattern.findFirstIn(status.getText).isEmpty) {
                ""
            }
            else {
                pattern.findFirstIn(status.getText).getOrElse("@MichaelCaraccio").tail
            },
            status.getText,
            status.getUser.getLang
            )
        }



        // Stream about tweets
        val tweetsStream = stream.map { status => (
            status.getId.toString,
            status.getUser.getId.toString,
            abs(murmurHash64A(status.getUser.getScreenName.getBytes)),
            new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(status.getCreatedAt),
            status.getRetweetCount.toString,
            status.getText
            )
        }


        // ************************************************************
        // Save user's informations in Cassandra
        // ************************************************************
        usersStream.foreachRDD(rdd => {
            rdd.saveToCassandra("twitter", "user_filtered", SomeColumns("user_twitter_id", "user_local_id", "user_name", "user_lang", "user_follow_count", "user_friends_count", "user_screen_name", "user_status_count"))

            println("Users saved : " + rdd.count())
        })

        // ************************************************************
        // Save communication's informations in Cassandra
        // ************************************************************
        commStream.foreachRDD(rdd => {

            // RDD -> Array()
            val tabValues = rdd.collect()

            // Collection of vertices (contains users)
            val collectionVertices = ArrayBuffer[(Long, String)]()

            // Collection of edges (contains communications between users)
            val collectionEdge = ArrayBuffer[Edge[String]]()

            // For each tweets in RDD
            for (item <- tabValues.toArray) {

                // Avoid single @ in message
                if (item._4 != "" && (item._6 == "en" || item._6 == "en-gb")) {

                    // Find multiple dest
                    val matches = pattern.findAllIn(item._5).toArray

                    // Sender ID
                    val sendID: Long = abs(gu murmurHash64A (item._3.getBytes))

                    collectionVertices += ((sendID, item._3))


                    // For each receiver in tweet
                    matches.foreach { destName => {

                        val user_dest_name = destName.drop(1)

                        // Generate Hash
                        val destID: Long = abs(gu murmurHash64A (user_dest_name.getBytes))

                        // Create each users and edges
                        collectionVertices += ((destID, user_dest_name))
                        collectionEdge += Edge(sendID, destID, item._1)

                        // TODO : Optimize save to cassandra with concatenate seq and save it when the loop is over
                        val collection = rdd.context.parallelize(Seq((item._1, item._2, sendID, destID)))

                        collection.saveToCassandra(
                            "twitter",
                            "users_communicate",
                            SomeColumns(
                                "tweet_id",
                                "user_send_twitter_id",
                                "user_send_local_id",
                                "user_dest_id"))
                    }
                    }
                }
            }

            // Convert vertices to RDD
            val VerticesRDD = ru ArrayToVertices(sc, collectionVertices)

            // Convert it to RDD
            val EdgeRDD = ru ArrayToEdges(sc, collectionEdge)

            // Create Graph
            val testGraph = time {
                Graph(VerticesRDD, EdgeRDD)
            }

            println("Comm saved in cassandra: " + testGraph.vertices.collect.length)
            println("Graph : " + testGraph.vertices.collect.length + " Vertices and " + testGraph.edges.collect.length + " edges")
            testGraph.vertices.collect.foreach(println(_))
            testGraph.edges.collect.foreach(println(_))

            val subGraphes = time {
                comUtils splitCommunity(testGraph, testGraph.vertices, true)
            }

        })


        // ************************************************************
        // Save tweet's informations in Cassandra
        // ************************************************************
        tweetsStream.foreachRDD(rdd => {

            // Getting current context
            val currentContext = rdd.context

            // RDD -> Array()
            val tabValues = rdd.collect()

            /*var test = rdd.map{status => (status._1,
                                          status._2,
                                          patternURL.replaceAllIn(status._3, ""),
                                          status._4,
                                          status._5,
                                          status._6,
                                          status._7)}*/

            // For each tweets in RDD
            for (item <- tabValues.toArray) {

                // New tweet value
                var newTweet = patternURL.replaceAllIn(item._6, "")
                newTweet = patternSmiley.replaceAllIn(newTweet, "")

                val collection = currentContext.parallelize(Seq((item._1, item._2, item._3, item._4, item._5, newTweet)))

                collection.saveToCassandra(
                    "twitter",
                    "tweet_filtered",
                    SomeColumns("tweet_id",
                        "user_twitter_id",
                        "user_local_id",
                        "tweet_create_at",
                        "tweet_retweet",
                        "tweet_text"
                    ))
            }

            println("Tweets saved : " + rdd.count())
        })

        ssc.start()
        ssc.awaitTermination()





        /*


        println("\n**************************************************************")
        println("                       SECOND EXAMPLE                         ")
        println("**************************************************************")

        println("\n--------------------------------------------------------------")
        println("First Step - Split community : \n" +
            "\t     Connected Components algorithm to find different\n" +
            "\t     communities")
        println("--------------------------------------------------------------")

        //time { comUtils cc(graph, graph.vertices) }

        val subGraphes = time { comUtils splitCommunity(graph, users, false) }

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

            // Initialize LDA
            println(color("\nCall InitLDA", RED))

            val topicSmoothing = 1.2
            val termSmoothing = 1.2

            // Set LDA parameters
            val lda = new LDA()
                .setK(numTopics)
                .setDocConcentration(topicSmoothing)
                .setTopicConcentration(termSmoothing)
                .setMaxIterations(numIterations)
                //.setOptimizer("online")

            // Create documents
            var firstDoc = ArrayBuffer[String]()
            firstDoc += "Concentration parameter commonly named for the prior placed"

            // Init LDA
            val mu = new MllibUtils(lda, sc, firstDoc, firstDoc)

            // First tweet
            mu newTweet("Concentration distributions topics Concentration")

            // Get documents and word's array
            val (newdoc:RDD[(Long, Vector)], newvocabArray) = time { mu createDocuments(sc, 0) }

            var ldaModel:DistributedLDAModel = lda.run(newdoc).asInstanceOf[DistributedLDAModel]

            // Find topics
            ldaModel = time { mu findTopics(ldaModel, newvocabArray, numWordsByTopics, true) }

            // Second tweet
            mu newTweet("October arrived, spreading a damp chill")

            val (newdoc2:RDD[(Long, Vector)], newvocabArray2) = time { mu createDocuments(sc, 0) }

            var ldaModel2:DistributedLDAModel = lda.run(newdoc2).asInstanceOf[DistributedLDAModel]

            // Find
            ldaModel2 = time { mu findTopics(ldaModel2, newvocabArray2, numWordsByTopics, true) }


            iComm +=1
        }

        // Generate Vertices
        val collectionVertices = ArrayBuffer[(Long, String)]()
        collectionVertices += ((2732329846L, "Michael"))
        collectionVertices += ((132988448L, "Jean"))

        // Convert it to RDD
        val VerticesRDD = ru ArrayToVertices(sc, collectionVertices)

        // Generate Hash
        val random = abs(gu murmurHash64A("MichaelCaraccio".getBytes))

        // Add edges
        val collectionEdge = ArrayBuffer[Edge[String]]()
        collectionEdge += Edge(random, 132988448L, "606460188367974400")
        collectionEdge += Edge(2732329846L, 2941487254L, "606461336986386435")
        collectionEdge += Edge(2732329846L, 601389784L, "606461384767897600")

        // Convert it to RDD
        val EdgeRDD = ru ArrayToEdges(sc, collectionEdge)

        // Create Graph
        val testGraph = Graph(VerticesRDD, EdgeRDD)

        testGraph.vertices.collect.foreach(println(_))
        testGraph.edges.collect.foreach(println(_))

        */
    }


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
     * @constructor time
     *
     *              timer for profiling block
     *
     * @param R $block - Block executed
     * @return Unit
     */
    def time[R](block: => R): R = {
        val t0 = System.nanoTime()
        val result = block // call-by-name
        val t1 = System.nanoTime()
        println("Elapsed time: " + (t1 - t0) / 1000000000.0 + " seconds")
        result
    }
}