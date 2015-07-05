//import com.google.gson.Gson

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.{SparkConf, SparkContext}
import utils._

import scala.math._


// Cassandra

import com.datastax.spark.connector._

// Regex

import org.apache.spark.mllib.clustering.{LDA, _}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

// To make some of the examples work we will also need RDD


object FinalProject {

    // Terminal color
    val RED = "\033[1;30m"
    val ENDC = "\033[0m"

    // Seed for murmurhash
    private val defaultSeed = 0xadc83b19L
    //var tweetsArray = new ArrayBuffer[Seq[String]]
    var dictionnary = new ArrayBuffer[String]

    var ldaModel: LDAModel = null
    var lda: LDA = null

    // Result will be stored in an array
    var result = new ArrayBuffer[String]

    var stockGraph: Graph[String, String] = null
    var currentTweets: String = ""

    var seqcommunities = List[(String, String, String, String, String, String, String)]()
    var seqtweetsStream = List[(String, String, String, String, String, String)]()
    var seqcommStream = List[(String, String, String, String)]()

    // Collection of vertices (contains users)
    var collectionVertices = new ArrayBuffer[(Long, String)]()

    // Collection of edges (contains communications between users)
    var collectionEdge = new ArrayBuffer[Edge[String]]()

    //var textBuffer = new ArrayBuffer[(String, String)]
    var textBuffer = collection.mutable.Map[String, String]()


    var counter = 0

    def color(str: String, col: String): String = "%s%s%s".format(col, str, ENDC)

    //var numTweetsCollected = 0L
    //var partNum = 0
    //var gson = new Gson()

    def main(args: Array[String]) {

        println("\n\n**************************************************************")
        println("******************        FinalProject      ******************")
        println("**************************************************************\n")

        val cu = new CassandraUtils
        val comUtils = new CommunityUtils
        //val gu = new GraphUtils
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
            .setMaster("local[8]")
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
        // can use them to generate OAuth credentials
        System.setProperty("twitter4j.oauth.consumerKey", tc.getconsumerKey())
        System.setProperty("twitter4j.oauth.consumerSecret", tc.getconsumerSecret())
        System.setProperty("twitter4j.oauth.accessToken", tc.getaccessToken())
        System.setProperty("twitter4j.oauth.accessTokenSecret", tc.getaccessTokenSecret())

        val ssc = new StreamingContext(sparkConf, Seconds(60))
        val stream = TwitterUtils.createStream(ssc, None, words)

        // Init SparkContext
        val sc = ssc.sparkContext
        //val cc = new CassandraSQLContext(ssc.sparkContext)

        val graph = loadGraphFromCassandra(cu, sc)

        // LDA parameters
        val topicSmoothing = 1.2
        val termSmoothing = 1.2
        val numTopics = 6
        val numIterations = 2 //10
        val numWordsByTopics = 8

        // Init LDA
        lda = new LDA()
            .setK(numTopics)
            .setDocConcentration(topicSmoothing)
            .setTopicConcentration(termSmoothing)
            .setMaxIterations(numIterations)
            .setOptimizer("online")

        println("Init LDA")
        val mu = new MllibUtils()

        // http://ochafik.com/blog/?p=806

        /**
         * LDA CREATED FROM CASSANDRA
         */

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
        // Save tweet's informations in Cassandra
        // ************************************************************
        tweetsStream.foreachRDD(rdd => {
            println("tweetsStream")

            // For each tweets in RDD
            for (item <- rdd) {

                // New tweet value
                var newTweet = patternURL.replaceAllIn(item._6, "")
                newTweet = patternSmiley.replaceAllIn(newTweet, "")

                seqtweetsStream = seqtweetsStream :+(item._1, item._2, item._3.toString, item._4, item._5, newTweet)
            }

            sc.parallelize(seqtweetsStream).saveToCassandra(
                "twitter",
                "tweet_filtered",
                SomeColumns("tweet_id",
                    "user_twitter_id",
                    "user_local_id",
                    "tweet_create_at",
                    "tweet_retweet",
                    "tweet_text"
                ))

            // reset
            seqtweetsStream = List[(String, String, String, String, String, String)]()
        })

        // ************************************************************
        // Save user's informations in Cassandra
        // ************************************************************
        usersStream.foreachRDD(rdd => {
            //rdd.saveToCassandra("twitter", "user_filtered", SomeColumns("user_twitter_id", "user_local_id", "user_name", "user_lang", "user_follow_count", "user_friends_count", "user_screen_name", "user_status_count"))

            //println("Users saved : " + rdd.count())
        })

        // ************************************************************
        // Save communication's informations in Cassandra
        // ************************************************************
        commStream.foreachRDD(rdd => {

            println("commstream appelé")

            /**
             * Enregistrement des messages dans cassandra
             */

            // For each tweets in RDD
            for (item <- rdd.cache()) {

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

                // Avoid single @ in message
                if (item._4 != "" && (item._6 == "en" || item._6 == "en-gb")) {

                    // Sender ID
                    val sendID: Long = abs(murmurHash64A(item._3.getBytes))

                    // Sender
                    collectionVertices += ((sendID, item._3))

                    textBuffer += (item._1 -> item._5)

                    // For each dest in tweet
                    pattern.findAllIn(item._5).foreach { destName => {

                        val user_dest_name = destName.drop(1)

                        // Generate Hash
                        val destID: Long = abs(murmurHash64A(user_dest_name.getBytes))

                        // Create each users and edges
                        collectionVertices += ((destID, user_dest_name))
                        collectionEdge += Edge(sendID, destID, item._1)

                        seqcommStream = seqcommStream :+(item._1, item._2, sendID.toString, destID.toString)

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


            sc.parallelize(seqcommStream).saveToCassandra(
                "twitter",
                "users_communicate",
                SomeColumns(
                    "tweet_id",
                    "user_send_twitter_id",
                    "user_send_local_id",
                    "user_dest_id"))

            // reset
            seqcommStream = List[(String, String, String, String)]()

            /**
             * Initialisation du graph
             */

            // Empty graph at first launch
            if (stockGraph == null) {
                // Convert vertices to RDD
                val VerticesRDD = ru ArrayToVertices(sc, collectionVertices)

                // Convert it to RDD
                val EdgeRDD = ru ArrayToEdges(sc, collectionEdge)

                stockGraph = Graph(VerticesRDD, EdgeRDD)
                stockGraph.unpersist()
                stockGraph.persist(StorageLevel.MEMORY_AND_DISK)
            }

            /**
             * Ajout des nouveaux Edges et Vertices dans le graph principal
             */

            time {
                stockGraph = Graph(stockGraph.vertices.union(sc.parallelize(collectionVertices)), stockGraph.edges.union(sc.parallelize(collectionEdge)))
            }

            println("TOTAL EDGES : " + stockGraph.edges.count())
            //println("TOTAL VERTICES : " + stockGraph.vertices.count())

            collectionVertices = new ArrayBuffer[(Long, String)]()
            collectionEdge = new ArrayBuffer[Edge[String]]()


            /**
             * Split main Graph in multiples communities
             */

            counter += 1

            val communityGraph = time {
                comUtils splitCommunity(stockGraph, stockGraph.vertices, false)
            }

            val (subgraphs, commIDs) = time {
                comUtils subgraphCommunities2(communityGraph, stockGraph.vertices, false)
            }

            /**
             * LDA
             */

            var cpt = 0
            for (sub <- subgraphs) {

                // Store cosine calculus
                var tabcosine = new ArrayBuffer[Double]()

                // Init variable for cassandra
                val T = counter
                val idComm = commIDs(cpt)
                val SG = cpt
                currentTweets = ""

                // Store tweet's text in result
                for (v <- sub.edges) {
                    if (textBuffer.keys.exists(_.toString == v.attr.toString)) {
                        result += textBuffer.get(v.attr.toString).toString.replaceAll("[!?.,:;<>)(]", " ")
                    }
                }

                /**
                 * If there's a new tweet in a community -> LDA
                 */

                if (result.nonEmpty) {

                    // Storage for cosines Similarity
                    var tab1 = new ArrayBuffer[Double]
                    var tab2 = new ArrayBuffer[Double]


                    println("Tweets by tweets -> Create documents and vocabulary")

                    //result.foreach(println(_))

                    result.foreach(x => {

                        val tweet = x
                            .toLowerCase.split("\\s")
                            .filter(_.length > 3)
                            .filter(_.forall(java.lang.Character.isLetter))

                        if (tweet.length > 0) {
                            for (t <- tweet) {
                                dictionnary += t
                            }
                            currentTweets = currentTweets.concat(tweet.mkString(" "))
                        }
                    })


                    // LDA for initial corpus
                    println("Creation du contenu")

                    // Create document
                    println("Creation du document")
                    val (res1: Seq[(Long, Vector)], res2: Array[String]) = createdoc(dictionnary.distinct, currentTweets)


                    // Start LDA
                    println("LDA Started")
                    ldaModel = lda.run(ssc.sparkContext.parallelize(res1).cache())
                    //val seqC = Seq[(String,String,String)]()

                    val seqC: Seq[(String, String, String, String)] = mu findTopics(ldaModel, res2, T, SG, numWordsByTopics, false)

                    //seqC.foreach(println(_))
                    // Save to cassandra
                    /*sc.parallelize(seqC).saveToCassandra(
                        "twitter",
                        "lda",
                        SomeColumns("t",
                            "sg",
                            "n_topic",
                            "words"
                        ))*/

                    println("LDA Finished\nFind Cosines Similarity")

                    ldaModel.describeTopics(3).foreach { case (terms, termWeights) =>
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

                    /*ldaModel.describeTopics(6).apply(biggestCosineIndex)._1.foreach { x =>
                        println(res2(x))
                    }*/
                }

                if (result.nonEmpty) {
                    // Pour chaques edges . On crée un Seq
                    for (v <- sub.edges) {
                        //println("T:" + T + " SG:" + SG + " IDCOM:" + idComm + " srcID:" + v.srcId + " dstID:" + v.dstId + " attr:" + v.attr + "LDA:" + tabcosine.mkString(";"))

                        seqcommunities = seqcommunities :+(T.toString, SG.toString, idComm.toString, v.srcId.toString, v.dstId.toString, v.attr, tabcosine.mkString(";"))
                    }

                    //seqcommunities.foreach(println(_))


                    // Save to cassandra
                    /*sc.parallelize(seqcommunities.toSeq).saveToCassandra(
                        "twitter",
                        "communities",
                        SomeColumns("t",
                            "sg",
                            "com_id",
                            "src_id",
                            "dst_id",
                            "attr",
                            "lda"
                        ))*/

                    seqcommunities = List[(String, String, String, String, String, String, String)]()

                }


                cpt += 1
                result.clear()
            }

            textBuffer.clear()

            // Enregistrement sur fichier JSON ( selon des fichiers )
            /*for (sub <- subgraphs) {
                val outputRDD = sub.edges.coalesce(1, shuffle = true).map(gson.toJson(_))
                val uri: URI = new URI(s"/home/mcaraccio/json/" + counter.toString + "/edges")
                outputRDD.saveAsTextFile(uri.toString)

                val outputRDDV = sub.vertices.coalesce(1, shuffle = true).map(gson.toJson(_))
                //val uriV: URI = new URI(s"/home/mcaraccio/json/" + counter.toString + "/vertices")
                ///outputRDDV.saveAsTextFile(uriV.toString)

                val ff = outputRDD ++ outputRDDV
                val uriM: URI = new URI(s"/home/mcaraccio/json/" + counter.toString + "/merge")
                ff.saveAsTextFile(uriM.toString)

            }*/
            //}

        })

        ssc.start()
        ssc.awaitTermination()
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

    /**
     *
     * @param cu
     * @param sc
     * @return
     */
    def loadGraphFromCassandra(cu: CassandraUtils, sc: SparkContext): Graph[String, String] = {

        //val (v, e) = { cu getAllCommunications(sc) }
        {
            cu getAllCommunicationsToGraph sc
        }

    }

    /**
     *
     * @param tokenizedCorpus
     * @param x
     * @return
     */
    def createdoc(tokenizedCorpus: ArrayBuffer[String], x: String): ((Seq[(Long, Vector)], Array[String])) = {

        val tokenizedTweet: Seq[String] = x.split(" ").toSeq

        // Map[String, Int] of words and theirs places in tweet
        val vocab: Map[String, Int] = tokenizedCorpus.zipWithIndex.toMap

        // MAP : [ Word ID , VECTOR [vocab.size, WordFrequency]]
        val documents: Map[Long, Vector] = vocab.map { case (tokens, id) =>

            val counts = new mutable.HashMap[Int, Double]()

            // Word ID
            val idx = vocab(tokens)

            // Count word occurancy
            counts(idx) = counts.getOrElse(idx, 0.0) + tokenizedTweet.count(_ == tokens)

            // Return word ID and Vector
            (id.toLong, Vectors.sparse(vocab.size, counts.toSeq))
        }
        (documents.toSeq, tokenizedCorpus.toArray)
    }

    /**
     * This method takes 2 equal length arrays of integers
     * It returns a double representing similarity of the 2 arrays
     * 0.9925 would be 99.25% similar
     * (x dot y) / ||X|| ||Y||
     *
     * @param x
     * @param y
     * @return cosine similarity
     */
    def cosineSimilarity(x: ArrayBuffer[Double], y: ArrayBuffer[Double]): Double = {
        require(x.length == y.length)
        dotProduct(x, y) / (magnitude(x) * magnitude(y))
    }

    /**
     * Return the dot product of the 2 arrays
     * e.g. (a[0]*b[0])+(a[1]*a[2])
     *
     * @param x
     * @param y
     * @return
     */
    def dotProduct(x: ArrayBuffer[Double], y: ArrayBuffer[Double]): Double = {
        (for ((a, b) <- x zip y) yield a * b) sum
    }

    /**
     * We multiply each element, sum it, then square root the result.
     *
     * @param x
     * @return  the magnitude of an array
     */
    def magnitude(x: ArrayBuffer[Double]): Double = {
        math.sqrt(x map (i => i * i) sum)
    }
}