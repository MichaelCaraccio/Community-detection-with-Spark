import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import utils._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


import scala.math._
import scala.reflect.ClassTag

//Log4J
import org.apache.log4j.{Level, Logger}

// Cassandra
import com.datastax.spark.connector._

// Regex
import scala.util.matching.Regex

// MLlib
import org.apache.spark.mllib.clustering.{LDA, _}
import org.apache.spark.mllib.linalg.{Vectors, Vector}



// TODO Unpersist stuff


object FinalProject {

    // /////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // CONSTANT
    // /////////////////////////////////////////////////////////////////////////////////////////////////////////////

    var MIN_VERTICES_PER_COMMUNITIES = 6    // Limit - Minimum vertices per communities
    var MIN_WORD_LENGTH = 3                 // Minimum word length in tweet
    var NBKCORE = 6                         // Number of core - K Core Decomposition algorithm
    var BATCH_SIZE = 1800                    // Batch size (in seconds)
    var CLEAN_GRAPH_MOD = 3                 // Clean stockGraph every CLEAN_GRAPH_MOD
    var CLEAN_GRAPH_NBKCORE = 2             // When clean graph is called, k-core decomposition is called

    val defaultSeed = 0xadc83b19L   // Seed for murmurhash - Do not change this value

    var dictionnary = new ArrayBuffer[String]()     // Store tweets
    var ldaModel: DistributedLDAModel = null        // LDA Model
    var lda: LDA = null                             // LDA object
    var stockGraph: Graph[String, String] = null    // Store every edges and vertices received by Twitter
    var currentTweets: String = ""                  // Current tweet received
    var counter = 1

    val RED = "\033[1;30m"                          // Terminal color RED
    val ENDC = "\033[0m"                            // Terminal end character


    def color(str: String, col: String): String = "%s%s%s".format(col, str, ENDC)



    def main(args: Array[String]) {

        //val comUtils = new CommunityUtils   // Community methods
        val ru = new RDDUtils               // Manipulate RDD class
        val tc = new TwitterConfig          // Login and password for Twitter
        //val mu = new MllibUtils()           // LDA class

        // LDA parameters
        val topicSmoothing = 1.2
        val termSmoothing = 1.2
        val numTopics = 10
        val numIterations = 20
        val numWordsByTopics = 12

        // Display only error messages
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)

        // Not displaying any messages
        //Logger.getLogger("org").setLevel(Level.OFF)
        //Logger.getLogger("akka").setLevel(Level.OFF)

        // Spark configuration
        val sparkConf = new SparkConf(true)
            .setAppName("FinalProject")
            .setMaster("yarn-client")
            //.set("spark.akka.frameSize", "250")
            //.set("spark.streaming.blockInterval", "2000")
            /*.set("spark.shuffle.service.enabled", "true") // needed fo dynamicAllocation
            .set("spark.dynamicAllocation.enabled", "true")
            .set("spark.dynamicAllocation.minExecutors", "16")
            .set("spark.dynamicAllocation.maxExecutor", "160")*/
            //.set("spark.akka.threads", "16")
            //.set("spark.streaming.receiver.maxRate", "0") // no limit on the rate
            //.set("spark.yarn.am.memory", "4g")
            //.set("spark.yarn.am.cores", "4")
            //.set("spark.io.compression.codec", "lzf") // improve shuffle performance
            //.set("spark.akka.threads", "10")
            //.set("spark.driver.cores", "8")
            //.set("spark.driver.memory", "32g")
            //.set("spark.executor.memory", "8g")
            .set("spark.driver.maxResultSize", "0") // no limit
            //.set("spark.executor.memory", "2g") // Amount of memory to use for the driver process
            //.set("spark.executor.memory", "2g") // Amount of memory to use per executor process
            .set("spark.cassandra.connection.host", "157.26.83.16") // Link to Cassandra
            .set("spark.cassandra.auth.username", "cassandra")
            .set("spark.cassandra.auth.password", "cassandra")
            //.set("spark.cassandra.output.batch.grouping.buffer.size", "10000")
            //.set("spark.cassandra.output.concurrent.writes", "10")
            //.set("spark.cassandra.output.batch.size.bytes", "2048")
            //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")// kryo is much faster
            //.set("spark.kryoserializer.buffer.mb", "256") // I serialize bigger objects
            //.set("spark.mesos.coarse", "true") // link provided
            .set("spark.akka.frameSize", "1000") // workers should be able to send bigger messages
            .set("spark.akka.askTimeout", "30"); // high CPU/IO load

        //sparkConf.registerKryoClasses(Array(classOf[MllibUtils]))

        //.set("spark.executor.extraJavaOptions", "-XX:MaxPermSize=512M -XX:+UseCompressedOops")

        // Set the system properties so that Twitter4j library used by twitter stream
        // can use them to generate OAuth credentials
        System.setProperty("twitter4j.oauth.consumerKey", tc.getconsumerKey())
        System.setProperty("twitter4j.oauth.consumerSecret", tc.getconsumerSecret())
        System.setProperty("twitter4j.oauth.accessToken", tc.getaccessToken())
        System.setProperty("twitter4j.oauth.accessTokenSecret", tc.getaccessTokenSecret())
        System.setProperty("twitter4j.http.retryCount", "5")
        System.setProperty("twitter4j.http.retryIntervalSecs", "1")
        System.setProperty("twitter4j.async.numThreads", "10")



        println("\n\n**************************************************************")
        println("******************        FinalProject      ******************")
        println("**************************************************************\n")








        val words = Array(" @") // Filters tweet stream by words

        // Pattern used to find users and filter tweets
        val pattern = new Regex("\\@\\w{3,}")
        val patternURL = new Regex("(http|ftp|https)://[A-Za-z0-9-_]+.[A-Za-z0-9-_:%&?/.=]+")
        val patternSmiley = new Regex("((?::|;|=)(?:-)?(?:\\)|D|P|3|O))")
        val patternCommonWords = new Regex("\\b(that|have|with|this|from|they|would|there|their|what|about|which|when|make|like|time|just|know|take|into|year|your|good|some|could|them|other|than|then|look|only|come|over|think|also|back|after|work|first|well|even|want|because|these|give|most|http|https|fpt)\\b")

        // Streaming context -> batch size
        val ssc = new StreamingContext(sparkConf, Seconds(BATCH_SIZE))
        val stream = TwitterUtils.createStream(ssc, None, words)

        // filter for english user only
        stream.filter(a => a.getUser.getLang.equals("en") || a.getUser.getLang.equals("en-GB"))

        // Group into larger batches
        val streamBatch = stream.window(Seconds(BATCH_SIZE), Seconds(BATCH_SIZE))

        // Init SparkContext
        val sc = ssc.sparkContext

        /**
         * LDA CREATED FROM CASSANDRA
         * Date comes from old tweets
         */
        println("\n*******************************************")
        println("Create corpus from Cassandra")
        println("*******************************************\n")

        // Get every tweets
        val rdd = sc.cassandraTable("twitter", "tweet_filtered").cache()

        rdd.select("tweet_text").as((i: String) => i).collect().foreach(x => {

            val preText = patternCommonWords.replaceAllIn(x.toLowerCase, "")

            val tweet = preText
                .toLowerCase.split("\\s")
                .filter(_.length > MIN_WORD_LENGTH)
                .filter(_.forall(java.lang.Character.isLetter))

            if (tweet.length > 0) {
                for (t <- tweet) {
                    dictionnary += t
                }

                //currentTweets = currentTweets.concat(tweet.mkString(" "))
            }
        })


        // Create RDD
        val dictRDDInit = sc.parallelize(dictionnary).cache()

        // Init LDA
        lda = new LDA()
            .setK(numTopics)
            .setDocConcentration(topicSmoothing)
            .setTopicConcentration(termSmoothing)
            .setMaxIterations(numIterations)
        // .setOptimizer("online") // works with Apache Spark 1.4 only

        // Create documents for LDA
        val (res1: Seq[(Long, Vector)], res2: Array[String], vocab: Map[String, Int]) = time { createdoc(dictRDDInit) }

        dictRDDInit.unpersist()

        println("Distinct words : " + dictionnary.distinct.size)

        if (res1.nonEmpty) {
            // Start LDA
            println("LDA Started")
            time {
                ldaModel = lda.run(ssc.sparkContext.parallelize(res1).cache())
            }
            println("LDA Finished\n")
        }






        // Stream about users
        val usersStream = streamBatch.map { status => (
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
        val commStream = streamBatch.map { status => (
            status.getId, //tweet_id
            status.getUser.getId.toString, // user_send_twitter_ID
            status.getUser.getScreenName, // user_send_name
            if (pattern.findFirstIn(status.getText).isEmpty) {
                ""
            }
            else {
                pattern.findFirstIn(status.getText).getOrElse("@MichaelCaraccio").tail
            },
            status.getText
            )
        }

        // Stream about tweets
        val tweetsStream = streamBatch.map { status => (
            status.getId.toString,
            status.getUser.getId.toString,
            abs(murmurHash64A(status.getUser.getScreenName.getBytes)),
            new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(status.getCreatedAt),
            status.getRetweetCount.toString,
            status.getText
            )
        }





        // /////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // STREAMING PART
        // Following code is called every batch interval
        // /////////////////////////////////////////////////////////////////////////////////////////////////////////////

        println("*******************************************")
        println("Streaming started")
        println("*******************************************\n")

        // ************************************************************
        // Save tweet's informations into Cassandra
        // ************************************************************
        tweetsStream.foreachRDD(rdd => {

            rdd.persist(StorageLevel.MEMORY_AND_DISK)

            // For each tweets in RDD
            val seqtweetsStream = rdd.collect().map(a => (a._1, a._2, a._3.toString, a._4, a._5, patternSmiley.replaceAllIn(patternURL.replaceAllIn(a._6, ""), ""))).toList


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
            rdd.unpersist()
        })

        // ************************************************************
        // Save user's informations in Cassandra
        // ************************************************************
        usersStream.persist(StorageLevel.MEMORY_AND_DISK).foreachRDD(rdd => {
            rdd.saveToCassandra("twitter", "user_filtered", SomeColumns("user_twitter_id", "user_local_id", "user_name", "user_lang", "user_follow_count", "user_friends_count", "user_screen_name", "user_status_count"))
        })

        // ************************************************************
        // Save communication's informations in Cassandra
        // ************************************************************
        commStream.persist(StorageLevel.MEMORY_AND_DISK).foreachRDD(rdd => {

            // Timer
            val t00 = System.nanoTime()

            // Collection of vertices (contains users)
            var collectionVertices = new ArrayBuffer[(Long, String)]()

            // Collection of edges (contains communications between users)
            var collectionEdge = new ArrayBuffer[Edge[String]]()

            val seqcommStream = new ListBuffer[(String, String, String, String)]()

            rdd.persist(StorageLevel.MEMORY_AND_DISK)

            /**
             * Enregistrement des messages dans cassandra
             */

            val textBuffer = rdd.collect().map { g => g._1 -> g._5 }.toMap

            // For each tweets in RDD
            for (item <- rdd.collect()) {

                // Avoid single @ in message, english only
                if (item._4.nonEmpty) {

                    // Sender ID
                    val sendID: Long = abs(murmurHash64A(item._3.getBytes))

                    // Sender
                    collectionVertices += ((sendID, item._3))

                    // For each dest in tweet
                    pattern.findAllIn(item._5).foreach { destName => {

                        val user_dest_name = destName.drop(1)

                        // Generate Hash
                        val destID: Long = abs(murmurHash64A(user_dest_name.getBytes))

                        if (sendID != destID) {
                            // Create each users and edges
                            collectionVertices += ((destID, user_dest_name))
                            collectionEdge += Edge(sendID, destID, item._1.toString)

                            seqcommStream.append((item._1.toString, item._2, sendID.toString, destID.toString))
                        }
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
            seqcommStream.clear()


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

            if (counter % CLEAN_GRAPH_MOD == 0) {
                println("################################################")
                println("Clean stockgraph")
                println("Before cleaning (edges): " + stockGraph.edges.count())

                stockGraph = time {
                    splitCommunity(stockGraph, stockGraph.vertices, CLEAN_GRAPH_NBKCORE, displayResult = false)
                }
                println("After cleaning (edges): " + stockGraph.edges.count())
                println("################################################")
            }

            val communityGraph = time {
                splitCommunity(stockGraph, stockGraph.vertices, NBKCORE, displayResult = false)
            }

            communityGraph.cache()

            val (subgraphs, commIDs) = time {
                subgraphCommunities(communityGraph, stockGraph.vertices, displayResult = false)
            }

            communityGraph.unpersist()

            /**
             * LDA
             */

            currentTweets = ""
            for (i <- subgraphs.indices) {
                // Timer
                //val t0 = System.nanoTime()

                // Current subgraph
                val sub = subgraphs(i).cache()

                val edges = sub.edges.collect()

                // Messages will be stored in an array
                val result = edges.map(message => textBuffer.getOrElse(message.attr.toLong, "").replaceAll("[!?.,:;<>)(]", " "))
                sub.unpersist()

                result.foreach(x => {

                    val preText = patternCommonWords.replaceAllIn(x.toLowerCase, "")

                    val tweet = preText
                        .toLowerCase.split("\\s")
                        .filter(_.length > MIN_WORD_LENGTH)
                        .filter(_.forall(java.lang.Character.isLetter))

                    if (tweet.nonEmpty) {
                        for (t <- tweet) {
                            dictionnary += t
                        }

                        //currentTweets = currentTweets.concat(tweet.mkString(" "))
                    }
                })
            }


            // Create document
            println("Create document")
            val dictRDD = sc.parallelize(dictionnary).cache()

            val (res1: Seq[(Long, Vector)], res2: Array[String], vocab: Map[String, Int]) = time { createdoc (dictRDD) }


            // Start LDA
            println("LDA Started")
            ldaModel = lda.run(ssc.sparkContext.parallelize(res1).cache())

            var seqC: Seq[(String, String, String, String)] = findTopics(ldaModel, res2, counter.toString, 0, numWordsByTopics, displayResult = true)

            seqC = seqC.map(a => (counter.toString, a._2, a._3, a._4))


            //Save to cassandra
            sc.parallelize(seqC).saveToCassandra(
                "twitter",
                "lda",
                SomeColumns("t",
                    "sg",
                    "n_topic",
                    "words"
                ))

            println("LDA Finished\nFind Cosines Similarity")


            var cpt = 0

            for (i <- subgraphs.indices) {

                println("\n\n:::::::::::::::::::::::::::::::::::")
                println("::::: Community N°" + i + " T: " + counter + " SG: " + cpt)
                println(":::::::::::::::::::::::::::::::::::")

                // Timer
                val t0 = System.nanoTime()

                // Current subgraph
                val sub = subgraphs(i).cache()

                val verticesCount = sub.vertices.count()

                if (verticesCount < MIN_VERTICES_PER_COMMUNITIES) {
                    println("Stop here : < " + MIN_VERTICES_PER_COMMUNITIES + " users")
                } else {
                    println("Number of users in community : " + verticesCount)

                    // Messages will be stored in an array
                    val result = sub.edges.collect().map(message => textBuffer.getOrElse(message.attr.toLong, "").replaceAll("[!?.,:;<>)(]", " "))

                    /**
                     * If there's a new tweet in a community -> LDA
                     */


                    if (result.nonEmpty) {

                        // Store cosine calculus
                        val tabcosine = new ArrayBuffer[Double]()

                        println("Words in current tweet: " + result.length)

                        // Storage for cosines Similarity
                        var tab1 = new ArrayBuffer[Double]()
                        var tab2 = new ArrayBuffer[Double]()


                        // Clean and Concatenate subgraph's tweets
                        //var cccc = result.map(t => patternCommonWords.replaceAllIn(t.toLowerCase, "").toLowerCase.split("\\s").filter(_.length > MIN_WORD_LENGTH).filter(_.forall(java.lang.Character.isLetter)).mkString(" ")).toString

                        currentTweets = ""
                        result.foreach(x => {

                            val preText = patternCommonWords.replaceAllIn(x.toLowerCase, "")

                            val tweet = preText
                                .toLowerCase.split("\\s")
                                .filter(_.length > MIN_WORD_LENGTH)
                                .filter(_.forall(java.lang.Character.isLetter))

                                currentTweets = currentTweets.concat(tweet.mkString(" "))

                        })


                        println("Call cosineSimilarity")
                        val res3: Seq[(Long, Vector)] = cosineSimilarity(dictRDD, vocab, currentTweets.split(" "))
                        println("outside cosineSimilarity")

                        ldaModel.describeTopics(numWordsByTopics).foreach { case (terms, termWeights) =>
                            terms.zip(termWeights).foreach { case (term, weight) =>
                                tab1 += res3.filter(x => x._1 == term).head._2.apply(term)
                                tab2 += weight.toDouble
                            }

                            // Store every cosine similarity
                            tabcosine += cosineSimilarity(tab1, tab2)
                        }

                        // Pour chaques edges . On crée un Seq qui contient le futur record pour cassandra
                        var seqcommunities = sub.edges.map(message => (counter.toString, verticesCount.toString, cpt.toString, commIDs(cpt).toString, message.srcId.toString, message.dstId.toString, message.attr, tabcosine.mkString(";"))).collect()

                        // Petit problème avec le counter qui ne se met pas a jour dans la method au dessus
                        seqcommunities = seqcommunities.map(a => (counter.toString, a._2, a._3, a._4, a._5, a._6, a._7, a._8))

                        //seqcommunities.foreach(println(_))

                        // Save to cassandra
                        sc.parallelize(seqcommunities.toSeq).saveToCassandra(
                            "twitter",
                            "communities",
                            SomeColumns("t",
                                "nbv",
                                "sg",
                                "com_id",
                                "src_id",
                                "dst_id",
                                "attr",
                                "lda"
                            ))
                    } else {
                        println("LDA wont process current document because it does not contains any words")
                    }

                    cpt += 1
                }

                val t1 = System.nanoTime()
                println("SubGraph N°: " + cpt + " processed in " + (t1 - t0) / 1000000000.0 + " seconds")
            }

            counter += 1

            val t11 = System.nanoTime()
            println("------------------------------------------------------------")
            println("BATCH FINISHED")
            println("Processed in " + (t11 - t00) / 1000000000.0 + " seconds")
            println("------------------------------------------------------------")
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

        if(magnitude(x) == 0.0 || magnitude(y) == 0.0)
            return 0.0

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
































    def splitCommunity(graph: Graph[String, String], users: RDD[(VertexId, (String))], NBKCORE: Int, displayResult: Boolean): Graph[String, String] = {

        println(color("\nCall SplitCommunity", RED))

        getKCoreGraph(graph, users, NBKCORE, displayResult).cache()
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
        var g = graph.cache().outerJoinVertices(graph.degrees)((vid, oldData, newData) => newData.getOrElse(0)).cache()

        println(color("\nCall KCoreDecomposition", RED))

        g = computeCurrentKCore(g, kmin).cache()

        val v = g.vertices.filter { case (vid, vd) => vd >= kmin }.cache()

        // Display informations
        if (displayResult) {
            val degrees = graph.degrees
            val numVertices = degrees.count()
            val testK = kmin
            val vCount = g.vertices.filter { case (vid, vd) => vd >= kmin }.count()
            val eCount = g.triplets.map { t => t.srcAttr >= testK && t.dstAttr >= testK }.count()

            /*logWarning(s"Number of vertices: $numVertices")
            logWarning(s"Degree sample: ${degrees.take(10).mkString(", ")}")
            logWarning(s"Degree distribution: " + degrees.map { case (vid, data) => (data, 1) }.reduceByKey(_ + _).collect().mkString(", "))
            logWarning(s"Degree distribution: " + degrees.map { case (vid, data) => (data, 1) }.reduceByKey(_ + _).take(10).mkString(", "))
            logWarning(s"K=$kmin, V=$vCount, E=$eCount")*/
        }

        // Create new RDD users
        val newUser = users.join(v).map {
            case (id, (username, rank)) => (id, username)
        }

        // Create a new graph
        val gra = Graph(newUser, g.edges)

        // Remove missing vertices as well as the edges to connected to them
        gra.subgraph(vpred = (id, username) => username != null).cache()
    }

    def computeCurrentKCore[ED: ClassTag](graph: Graph[Int, ED], k: Int) = {
        println("Computing kcore for k=" + k)
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

        // Note that initial message should have no effect
        Pregel(graph, 0)(vProg, sendMsg, mergeMsg)
    }

    def subgraphCommunities(graph: Graph[String, String], users: RDD[(VertexId, (String))], displayResult: Boolean): (Array[Graph[String, String]], Array[Long]) = {

        println(color("\nCall subgraphCommunities", RED))

        // Find the connected components
        val cc = time {
            graph.connectedComponents().vertices.cache()
        }

        // Join the connected components with the usernames and id
        // The result is an RDD not a Graph
        val ccByUsername = users.join(cc).map {
            case (id, (username, cci)) => (id, username, cci)
        }.cache()

        // Print the result
        val lowerIDPerCommunity = ccByUsername.map { case (id, username, cci) => cci }.distinct().cache()

        // Result will be stored in an array
        //var result = new ArrayBuffer[Graph[String, String]]()
        println("--------------------------")
        println("Total community found: " + lowerIDPerCommunity.count())
        println("--------------------------")


        val collectIDsCommunity = lowerIDPerCommunity.collect()

        val result = collectIDsCommunity.map(colID => Graph(ccByUsername.filter {
            _._3 == colID
        }.map { case (id, username, cc) => (id, username) }, graph.edges).subgraph(vpred = (id, username) => username != null).cache())

        // Display communities
        if (displayResult) {
            println("\nCommunities found " + result.length)
            for (community <- result) {
                println("-----------------------")
                community.edges.collect().foreach(println(_))
                community.vertices.collect().foreach(println(_))
            }
        }

        cc.unpersist()
        lowerIDPerCommunity.unpersist()

        (result, collectIDsCommunity)
    }













































    def createdoc(tokenizedCorpus: RDD[String]): ((Seq[(Long, Vector)], Array[String], Map[String, Int])) = {

        println(color("\nCall createdoc", RED))

        // Choose the vocabulary.
        // termCounts: Sorted list of (term, termCount) pairs
        val termCounts: Array[(String, Long)] =
            tokenizedCorpus.map(_ -> 1L).reduceByKey(_ + _).collect().sortBy(-_._2)

        // vocabArray: Chosen vocab (removing common terms)
        val numStopwords = 0
        val vocabArray: Array[String] =
            termCounts.takeRight(termCounts.length - numStopwords).map(_._1)

        // vocab: Map term -> term index
        val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

        //val tokenCollected = tokenizedCorpus.collect()

        // count the occurrence of each word
        //val wordCounts = tokenizedCorpus.map((_, 1)).reduceByKey(_ + _)


        // MAP : [ Word ID , VECTOR [vocab.size, WordFrequency]]
        val documents: Map[Long, Vector] = vocab.map { case (tokens, id) =>

            val counts = new mutable.HashMap[Int, Double]()

            // Word ID
            val idx = vocab(tokens)

            // Count word occurancy
                //wordCounts.filter(_._1 == tokens).first()._2
            counts(idx) = counts.getOrElse(idx, 0.0) + 1.0

            // Return word ID and Vector
            (id.toLong, Vectors.sparse(vocab.size, counts.toSeq))
        }

        (documents.toSeq, tokenizedCorpus.collect(), vocab)
    }


    def cosineSimilarity(tokenizedCorpus: RDD[String], vocab: Map[String, Int], tokenizedTweet: Array[String]): (Seq[(Long, Vector)]) = {

        println(color("\nCall cosineSimilarity", RED))

        val document: Map[Long, Vector] = vocab.map { case (tokens, id) =>

            val counts2 = new mutable.HashMap[Int, Double]()

            // Word ID
            val idx = vocab(tokens)

            // Count word occurancy
            counts2(idx) = counts2.getOrElse(idx, 0.0) + tokenizedTweet.count(_ == tokens).toDouble

            // Return word ID and Vector
            (id.toLong, Vectors.sparse(vocab.size, counts2.toSeq))
        }

        document.toSeq
    }

    /**
     * @constructor findTopics
     *
     *              Set currentTweet attribut and add the new tweet to the dictionnary
     *
     * @param LDAModel $ldaModel - LDA Model (LocalModel)
     * @param Array[String] $vocabArray - Contains all distinct words set to LDA
     * @param Int $numWordsByTopics -
     * @param Boolean $displayResult - Display result in console
     *
     * @return LDAModel
     */
    def findTopics(ldaModel: LDAModel, vocabArray: Array[String], T: String, SG: Int, numWordsByTopics: Int, displayResult: Boolean): Seq[(String, String, String, String)] = {

        println(color("\nCall findTopics", RED))

        println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")

        val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = numWordsByTopics)

        var it = 0
        var seqC = List[(String, String, String, String)]()

        // Print topics, showing top-weighted x terms for each topic.
        topicIndices.foreach { case (terms, termWeights) =>

            if (displayResult)
                println("TOPICS:")

            val tabTopics = terms.zip(termWeights).map(vector => vocabArray(vector._1.toInt).toString).mkString(";")

            if (displayResult) {
                terms.zip(termWeights).foreach { case (term, weight) =>
                    println(s"${vocabArray(term.toInt)}\t\t$weight")
                }
            }

            seqC = seqC :+(T, SG.toString, it.toString, tabTopics)

            println("T: " + T + " SG: " + SG + "TopicN: " + it + " c: " + tabTopics)
            it += 1

            if (displayResult)
                println()

        }
        seqC.toSeq
    }
}