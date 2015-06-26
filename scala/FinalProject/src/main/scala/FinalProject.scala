import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}
import utils._
import org.apache.spark.Accumulator
import org.apache.spark.streaming.twitter.TwitterUtils
import java.nio.charset.StandardCharsets

import scala.math._

// Cassandra

import com.datastax.spark.connector._

// Regex

import org.apache.spark.mllib.clustering.{LDA, _}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import scala.util.matching.Regex

// To make some of the examples work we will also need RDD


// Useful links
// http://ampcamp.berkeley.edu/big-data-mini-course/graph-analytics-with-graphx.html
// https://spark.apache.org/docs/latest/graphx-programming-guide.html

object FinalProject {

    // Terminal color
    val RED = "\033[1;30m"
    val ENDC = "\033[0m"

    // Seed for murmurhash
    private val defaultSeed = 0xadc83b19L
    var tweetsArray = new ArrayBuffer[Seq[String]]
    var dictionnary = new ArrayBuffer[String]
    //var results = new ArrayBuffer[(Seq[(Long, Vector)], Array[String])]
    var ldaModel: LDAModel = null
    var lda: LDA = null
    //var vocab: Map[String, Int] = null

    def color(str: String, col: String): String = "%s%s%s".format(col, str, ENDC)

    def main(args: Array[String]) {

        println("\n\n**************************************************************")
        println("******************        FinalProject      ******************")
        println("**************************************************************\n")

        val cu = new CassandraUtils
        val comUtils = new CommunityUtils
        val gu = new GraphUtils
        val ru = new RDDUtils
        val tc = new TwitterConfig

        // Display only warning and infos messages
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)

        // Not displaying infos messages
        //Logger.getLogger("org").setLevel(Level.OFF)
        //Logger.getLogger("akka").setLevel(Level.OFF)

        // Spark configuration
        val sparkConf = new SparkConf(true)
            .setMaster("local[16]")
            .setAppName("FinalProject")
            //.set("spark.driver.cores", "4")
            .set("spark.driver.maxResultSize", "0") // no limit
            //.set("spark.executor.memory", "2g") // Amount of memory to use for the driver process
            //.set("spark.executor.memory", "2g") // Amount of memory to use per executor process
            .set("spark.cassandra.connection.host", "157.26.83.16") // Link to Cassandra
            .set("spark.cassandra.auth.username", "cassandra")
            .set("spark.cassandra.auth.password", "cassandra")
            .set("spark.executor.extraJavaOptions", "-XX:MaxPermSize=256M -XX:+UseCompressedOops")

        // Filters by words that contains @
        val words = Array(" @")

        // Pattern used to find users
        val pattern = new Regex("\\@\\w{3,}")
        val patternURL = new Regex("(http|ftp|https)://[A-Za-z0-9-_]+.[A-Za-z0-9-_:%&?/.=]+")
        val patternSmiley = new Regex("((?::|;|=)(?:-)?(?:\\)|D|P|3|O))")

        System.setProperty("twitter4j.http.retryCount", "3")
        System.setProperty("twitter4j.http.retryIntervalSecs", "10")
        System.setProperty("twitter4j.async.numThreads", "10")

        // Set the system properties so that Twitter4j library used by twitter stream
        // can use them to generat OAuth credentials
        System.setProperty("twitter4j.oauth.consumerKey", tc.getconsumerKey)
        System.setProperty("twitter4j.oauth.consumerSecret", tc.getconsumerSecret)
        System.setProperty("twitter4j.oauth.accessToken", tc.getaccessToken)
        System.setProperty("twitter4j.oauth.accessTokenSecret", tc.getaccessTokenSecret)

        val ssc = new StreamingContext(sparkConf, Seconds(10))
        val stream = TwitterUtils.createStream(ssc, None, words)

        // Init SparkContext
        val sc = ssc.sparkContext
        val cc = new CassandraSQLContext(ssc.sparkContext)

        val graph = loadGraphFromCassandra(cu, sc)

        //graph.edges.toJavaRDD().saveAsTextFile("/sds")

        /*val subGraphes = time {
            comUtils splitCommunity(graph, graph.vertices, true)
        }*/


        val topicSmoothing = 1.2
        val termSmoothing = 1.2
        val numTopics = 6
        val numIterations = 2 //10
        val numWordsByTopics = 8
        val numStopwords = 0

        // Set LDA parameters

        lda = new LDA()
            .setK(numTopics)
            .setDocConcentration(topicSmoothing)
            .setTopicConcentration(termSmoothing)
            .setMaxIterations(numIterations)
            .setOptimizer("online")

        println("Init LDA")

        val mu = new MllibUtils()

        //val rdd = sc.cassandraTable("twitter", "tweet_filtered").select("tweet_text").cache()

        /*

        // Create documents
        var firstDoc = ArrayBuffer[String]()
        firstDoc += "Concentration parameter commonly named for the prior placed"
        dictionnary += "Concentration parameter commonly named for the prior placed"

        // Init LDA
        val mu = new MllibUtils(lda, sc, firstDoc, firstDoc)

        // First tweet
        mu newTweet "Concentration distributions topics Concentration"
        dictionnary += "Concentration distributions topics Concentration"

        // Get documents and word's array
        val (newdoc: RDD[(Long, Vector)], newvocabArray) = time {
            mu createDocuments(sc, 0)
        }

        ldaModel = lda.run(newdoc)


        // Find topics
        ldaModel = time {
            mu findTopics(ldaModel, newvocabArray, numWordsByTopics, true)
        }

        */

        // http://ochafik.com/blog/?p=806


        /********************************************************************
        // LDA CREATED FROM CASSANDRA
        ********************************************************************/
        /*
        // Get every tweets
        val rdd = sc.cassandraTable("twitter", "tweet_filtered").select("tweet_text").cache()

        println("Tweets by tweets -> Create documents and vocabulary")
        rdd.select("tweet_text").as((i: String) => i).cache().foreach(x => {

            val tweet = x
                .toLowerCase.split("\\s")
                .filter(_.length > 3)
                .filter(_.forall(java.lang.Character.isLetter)).mkString(" ")

            if (tweet.length > 1)
                dictionnary += tweet
        })

        var tab1 = new ArrayBuffer[Double]
        var tab2 = new ArrayBuffer[Double]

        var tabcosine = new ArrayBuffer[Double]


        // LDA for initial corpus
        println("Creation du contenu")


        val dictDistinct = dictionnary.distinct


        // Create document
        println("Creation du document")
        val (res1:Seq[(Long, Vector)], res2:Array[String]) = createdoc(dictDistinct, dictionnary.toString())

        // Start LDA
        println("LDA Started")
        ldaModel = lda.run(ssc.sparkContext.parallelize(res1).cache())
        ldaModel = time {
            mu findTopics(ldaModel, res2, numWordsByTopics, true)
        }
        println("LDA Finished\nDisplay results")
        val topicIndices = ldaModel.describeTopics(5)
        topicIndices.foreach { case (terms, termWeights) =>
            terms.zip(termWeights).foreach { case (term, weight) =>
                tab1 += res1.filter(x => x._1 == term).head._2.apply(term)
                tab2 += weight
            }

            // Store every cosine similarity
            tabcosine += cosineSimilarity(tab1, tab2)

            // Reset array
            tab1 = new ArrayBuffer[Double]
            tab2 = new ArrayBuffer[Double]
        }

        val biggestCosineIndex: Int = tabcosine.indexOf(tabcosine.max)
        println("Most similarity found with this topic: " + tabcosine(biggestCosineIndex))
        println("Topic words : ")

        ldaModel.describeTopics(6).apply(biggestCosineIndex)._1.foreach { x =>
            println(res2(x))
        }

        tabcosine = new ArrayBuffer[Double]

        */




        /*for(i <- dictionnary.indices) {
            println(i)

            s = (s ++ dictionnary(i).split(" ")).distinct

            //results += createdoc(s, dictionnary(i), 0)
            val (res1:Seq[(Long, Vector)], res2:Array[String]) = createdoc(s, dictionnary(i))

            ldaModel = lda.run(ssc.sparkContext.parallelize(res1).cache())
            ldaModel = time {
                mu findTopics(ldaModel, res2, numWordsByTopics, true)
            }

            val topicIndices = ldaModel.describeTopics()
            topicIndices.foreach { case (terms, termWeights) =>
                terms.zip(termWeights).foreach { case (term, weight) =>
                    tab1 += res1.filter(x => x._1 == term).head._2.apply(term)
                    tab2 += weight
                }

                // Store every cosine similarity
                tabcosine += cosineSimilarity(tab1, tab2)

                // Reset array
                tab1 = new ArrayBuffer[Double]
                tab2 = new ArrayBuffer[Double]
            }

            val biggestCosineIndex: Int = tabcosine.indexOf(tabcosine.max)
            println("Most similarity found with this topic: " + tabcosine(biggestCosineIndex))
            println("Topic words : ")

            ldaModel.describeTopics(6).apply(biggestCosineIndex)._1.foreach { x =>
                println(res2(x))
            }

            tabcosine = new ArrayBuffer[Double]

        }*/























        println("---- Streaming started ----")

        // Stream about users
        val usersStream = stream.map { status => (
            status.getUser.getId.toString,
            abs(murmurHash64A(status.getUser.getScreenName.getBytes)),
            status.getUser.getName,
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
            //rdd.saveToCassandra("twitter", "user_filtered", SomeColumns("user_twitter_id", "user_local_id", "user_name", "user_lang", "user_follow_count", "user_friends_count", "user_screen_name", "user_status_count"))

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
                    val sendID: Long = abs(gu murmurHash64A item._3.getBytes)

                    collectionVertices += ((sendID, item._3))


                    // For each receiver in tweet
                    matches.foreach { destName => {

                        val user_dest_name = destName.drop(1)

                        // Generate Hash
                        val destID: Long = abs(gu murmurHash64A user_dest_name.getBytes)

                        // Create each users and edges
                        collectionVertices += ((destID, user_dest_name))
                        collectionEdge += Edge(sendID, destID, item._1)

                        // TODO : Optimize save to cassandra with concatenate seq and save it when the loop is over
                        val collection = rdd.context.parallelize(Seq((item._1, item._2, sendID, destID)))

                        /*collection.saveToCassandra(
                            "twitter",
                            "users_communicate",
                            SomeColumns(
                                "tweet_id",
                                "user_send_twitter_id",
                                "user_send_local_id",
                                "user_dest_id"))*/
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

            println("Comm saved in cassandra: " + testGraph.vertices.collect().length)
            println("Graph : " + testGraph.vertices.collect().length + " Vertices and " + testGraph.edges.collect().length + " edges")
            testGraph.vertices.collect().foreach(println(_))
            testGraph.edges.collect().foreach(println(_))

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

                /*collection.saveToCassandra(
                    "twitter",
                    "tweet_filtered",
                    SomeColumns("tweet_id",
                        "user_twitter_id",
                        "user_local_id",
                        "tweet_create_at",
                        "tweet_retweet",
                        "tweet_text"
                    ))*/
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
     *              Murmur is a family of good general purpose hashing functions, suitable for non-cryptographic usage. As stated by Austin Appleby, MurmurHash provides the following benefits:
     *              - good distribution (passing chi-squared tests for practically all keysets & bucket sizes.
     *              - good avalanche behavior (max bias of 0.5%).
     *              - good collision resistance (passes Bob Jenkin's frog.c torture-test. No collisions possible for 4-byte keys, no small (1- to 7-bit) differentials).
     *              - great performance on Intel/AMD hardware, good tradeoff between hash quality and CPU consumption.
     *
     *              Source : http://stackoverflow.com/questions/11899616/murmurhash-what-is-it
     *
     * @param Seq[Byte] - $data
     * @param Long - $seed
     * @return Long - Return hash
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

    def loadGraphFromCassandra(cu: CassandraUtils, sc: SparkContext): Graph[String, String] = {

        //val (v, e) = { cu getAllCommunications(sc) }
        {
            cu getAllCommunicationsToGraph sc
        }

    }

    def createdoc(tokenizedCorpus:ArrayBuffer[String] ,x: String): ((Seq[(Long, Vector)], Array[String])) = {
        /*val tokenizedCorpus: Seq[String] =
            dict.map(_.toLowerCase.split("\\s")).flatMap(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter))).toSeq
*/
        /*println("tokenizedCorpus")
        dict.map(_.toLowerCase.split("\\s")).flatMap(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter))).foreach(println(_))
        println("tokenizedCorpus")*/
        val tokenizedTweet: Seq[String] = x.split(" ").toSeq
        println("tokenizedTweet finished")

        // Choose the vocabulary
        // termCounts: Sorted list of (term, termCount) pairs
        // http://stackoverflow.com/questions/15487413/scala-beginners-simplest-way-to-count-words-in-file
        /*val termCounts = tokenizedCorpus.flatMap(_.split("\\W+")).foldLeft(Map.empty[String, Int]) {
            (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
        }.toArray

        // vocabArray contains all distinct words
        val vocabArray: Array[String] = termCounts.takeRight(termCounts.length - numStopwords).map(_._1)
*/

        // Map[String, Int] of words and theirs places in tweet
        val vocab: Map[String, Int] = tokenizedCorpus.zipWithIndex.toMap
        println("vocab finished")
        println("vsize:" + vocab.size)
        //vocab.foreach(println(_))

        // MAP : [ Word ID , VECTOR [vocab.size, WordFrequency]]
        val documents: Map[Long, Vector] =
            vocab.map { case (tokens, id) =>
                val counts = new mutable.HashMap[Int, Double]()
                //println(documents.size)
                // Word ID
                val idx = vocab(tokens)

                // Count word occurancy
                counts(idx) = counts.getOrElse(idx, 0.0) + tokenizedTweet.count(_ == tokens)

                // Return word ID and Vector
                (id.toLong, Vectors.sparse(vocab.size, counts.toSeq))
            }

        //documents.foreach(println(_))


        (documents.toSeq, tokenizedCorpus.toArray)
    }

    /*
   * This method takes 2 equal length arrays of integers
   * It returns a double representing similarity of the 2 arrays
   * 0.9925 would be 99.25% similar
   * (x dot y)/||X|| ||Y||
   */
    def cosineSimilarity(x: ArrayBuffer[Double], y: ArrayBuffer[Double]): Double = {
        require(x.length == y.length)
        dotProduct(x, y) / (magnitude(x) * magnitude(y))
    }

    /*
     * Return the dot product of the 2 arrays
     * e.g. (a[0]*b[0])+(a[1]*a[2])
     */
    def dotProduct(x: ArrayBuffer[Double], y: ArrayBuffer[Double]): Double = {
        (for ((a, b) <- x zip y) yield a * b) sum
    }

    /*
     * Return the magnitude of an array
     * We multiply each element, sum it, then square root the result.
     */
    def magnitude(x: ArrayBuffer[Double]): Double = {
        math.sqrt(x map (i => i * i) sum)
    }

    def queryText(cc: CassandraSQLContext, id: String): String = {
        val query = cc.sql("SELECT tweet_text from twitter.tweet_filtered where tweet_id = " + id)
        //val query = sc.cassandraTable("twitter", "tweet_filtered").select("tweet_text").where("tweet_id = ?",id)
        query.first().getString(0)
    }

}