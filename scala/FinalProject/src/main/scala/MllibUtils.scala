package MllibUtils

import scala.collection.mutable
import scala.collection.mutable.HashMap
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, DenseMatrix, Matrix, Vectors}
import scala.collection.mutable.ArrayBuffer
import Array._
//import scalaz._, Scalaz._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector

class MllibUtils(_lda:LDA, _sc:SparkContext, _dictionnary:ArrayBuffer[String],_currentTweet:ArrayBuffer[String]) {

    // http://stackoverflow.com/questions/2440134/is-this-the-proper-way-to-initialize-null-references-in-scala
    var lda:LDA = _lda
    var dictionnary:ArrayBuffer[String] = _dictionnary
    var currentTweet:ArrayBuffer[String] = _currentTweet

    // Text Color
    val RED = "\033[1;30m"
    val ENDC = "\033[0m"

    var currentTweetRDD:RDD[String] = _sc.parallelize(_dictionnary)
    var sc:SparkContext = _sc


    def color(str: String, col: String): String = "%s%s%s".format(col, str, ENDC)



    /**
     * @constructor getLDA
     *
     * Topic models automatically infer the topics discussed in a collection of documents. These topics can be used
     * to summarize and organize documents, or used for featurization and dimensionality reduction in later stages
     * of a Machine Learning (ML) pipeline.
     *
     * LDA is not given topics, so it must infer them from raw text. LDA defines a topic as a distribution over words.
     *
     * @param Int $numTopics - Number of topics
     * @param Int $numIterations - Number of iterations
     * @param Int $numWordsByTopics - Number of Words by topics
     * @param Int $numStopwords - Number of stop words
     *
     * @return Unit
     */
    def initLDA(numTopics:Int, numIterations:Int, numStopwords:Int) {

        println(color("\nCall InitLDA", RED))

        val topicSmoothing = 1.2
        val termSmoothing = 1.2

        // Set LDA parameters
        lda = new LDA()
            .setK(numTopics)
            .setDocConcentration(topicSmoothing)
            .setTopicConcentration(termSmoothing)
            .setMaxIterations(numIterations)
            //.setOptimizer("online")

    }

    def addToDictionnary(newTweet:String): Unit ={
        dictionnary += newTweet
    }

    def newTweet(newTweet:String): Unit ={
        currentTweet = new ArrayBuffer[String]()
        currentTweet += newTweet
        currentTweetRDD = sc.parallelize(currentTweet)

        // Add tweet to dictionnary
        addToDictionnary(newTweet)

        currentTweetRDD.collect.foreach(println(_))
    }


    def findTopics(ldaModel:DistributedLDAModel, vocabArray: Array[String], numWordsByTopics:Int, displayResult:Boolean) : DistributedLDAModel= {

        println(color("\nCall findTopics", RED))

        println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")

        // Print topics, showing top-weighted 5 terms for each topic.
        if(displayResult) {
            val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = numWordsByTopics)
            topicIndices.foreach { case (terms, termWeights) =>
                println("TOPICS:")
                terms.zip(termWeights).foreach { case (term, weight) =>
                    println(s"${vocabArray(term.toInt)}\t\t$weight")
                }
                println()
            }
        }
        ldaModel
    }


    def createDocuments(sc:SparkContext, numStopwords:Int): (RDD[(Long, Vector)], Array[String]) = {

        println(color("\nCall createDocuments", RED))

        val corpus:RDD[String] =  sc.parallelize(dictionnary)

        // Split every tweets's text into terms (words) and then remove :
        // -> (a) non-alphabetic terms
        // -> (b) short terms with < 4 characters
        // -> (c) to lower
        val tokenizedCorpus: RDD[Seq[String]] =
            corpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))

        // Split tweet's text into terms (words) and then remove :
        // -> (a) non-alphabetic terms
        // -> (b) short terms with < 4 characters
        // -> (c) to lower
        val tokenizedTweet: RDD[Seq[String]] =
            currentTweetRDD.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))


        // Choose the vocabulary
        //   termCounts: Sorted list of (term, termCount) pairs
        val termCounts: Array[(String, Long)] = tokenizedCorpus.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)

        // vocabArray contains all distinct words
        val vocabArray: Array[String] = termCounts.takeRight(termCounts.size - numStopwords).map(_._1)


        // Map[String, Int] of words and theirs places in tweet
        val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap
        //vocab.foreach(println(_))

        // MAP : [ Word ID , VECTOR [vocab.size, WordFrequency]]
        val documents: Map[Long, Vector] =
            vocab.map { case (tokens, id) =>
                val counts = new mutable.HashMap[Int, Double]()

                // Word ID
                val idx = vocab(tokens)

                // Count word occurancy
                counts(idx) = counts.getOrElse(idx, 0.0) + tokenizedTweet.collect.flatten.count(_ == tokens)

                // Return word ID and Vector
                (id.toLong, Vectors.sparse(vocab.size, counts.toSeq))
            }

        // Transform it to RDD
        val documentsRDD = sc.parallelize(documents.toSeq)

        // Display RDD
        documentsRDD.collect.foreach(println(_))

        // Return
        (documentsRDD, vocabArray)
    }
}