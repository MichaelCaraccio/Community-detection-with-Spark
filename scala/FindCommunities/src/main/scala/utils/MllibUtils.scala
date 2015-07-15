package utils

import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Topic models automatically infer the topics discussed in a collection of documents. These topics can be used
 * to summarize and organize documents, or used for featurization and dimensionality reduction in later stages
 * of a Machine Learning (ML) pipeline.
 *
 * LDA is not given topics, so it must infer them from raw text. LDA defines a topic as a distribution over words.
 */
class MllibUtils {

    // Terminal Color
    val RED = "\033[1;30m"
    val ENDC = "\033[0m"

    def color(str: String, col: String): String = "%s%s%s".format(col, str, ENDC)

    def createdoc(tokenizedCorpus: RDD[String]): ((Seq[(Long, Vector)], Array[String], Map[String, Int], Array[String])) = {

        println(color("\nCall createdoc", RED))

        // Choose the vocabulary.
        // termCounts: Sorted list of (term, termCount) pairs
        val termCounts: Array[(String, Long)] =
            tokenizedCorpus.map(_ -> 1L).reduceByKey(_ + _).collect().sortBy(-_._2)

        // vocabArray: Chosen vocab (removing common terms)
        val numStopwords = 20
        val vocabArray: Array[String] =
            termCounts.takeRight(termCounts.length - numStopwords).map(_._1)

        // vocab: Map term -> term index
        val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

        val tokenCollected = tokenizedCorpus.collect()


        // MAP : [ Word ID , VECTOR [vocab.size, WordFrequency]]
        val documents: Map[Long, Vector] = vocab.map { case (tokens, id) =>

            val counts = new mutable.HashMap[Int, Double]()

            // Word ID
            val idx = vocab(tokens)

            // Count word occurancy
            counts(idx) = counts.getOrElse(idx, 0.0) + tokenCollected.count(_ == tokens)

            // Return word ID and Vector
            (id.toLong, Vectors.sparse(vocab.size, counts.toSeq))
        }

        (documents.toSeq, tokenizedCorpus.collect(), vocab, tokenizedCorpus.collect())
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