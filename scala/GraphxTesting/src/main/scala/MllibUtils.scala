package MllibUtils

import scala.collection.mutable
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, DenseMatrix, Matrix, Vectors}

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

class MllibUtils {


    val RED = "\033[1;30m"
    val ENDC = "\033[0m"

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
     * @param SparkContext sc - SparkContext
     * @param Int $numTopics - Number of topics
     * @param Int $numIterations - Number of iterations
     * @param Int $numWordsByTopics - Number of Words by topics
     * @param Int $numStopwords - Number of stop words
     *
     * @return Unit
     */
    def getLDA(sc:SparkContext, corpus:RDD[String], numTopics:Int, numIterations:Int, numWordsByTopics:Int, numStopwords:Int): Unit ={

        println(color("\nCall GetLDA" , RED))

        // Split text into terms (words) and then remove :
        // -> (a) non-alphabetic terms
        // -> (b) short terms with < 4 characters
        // -> (c) the most common 20 terms (as “stopwords”)
        val tokenized: RDD[Seq[String]] =
            corpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))



        // Choose the vocabulary
        //   termCounts: Sorted list of (term, termCount) pairs
        val termCounts: Array[(String, Long)] =
            tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)


        //println("\nvocabArray\n")
        //termCounts.foreach(x => println("Word: "+x._1.toString + "   Count: " +x._2.toString))

        //   vocabArray: Chosen vocab (removing common terms)
        val vocabArray: Array[String] =
            termCounts.takeRight(termCounts.size - numStopwords).map(_._1)
        //   vocab: Map term -> term index
        val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

        //println("\ntokenized\n")
        //tokenized.foreach(println(_))
        /*println("\nvocabArray\n")
        vocabArray.foreach(println(_))
        println("\nvocab\n")
        vocab.foreach(println(_))*/

        // Convert documents into term count vectors
        val documents: RDD[(Long, Vector)] =
            tokenized.zipWithIndex.map { case (tokens, id) =>
                val counts = new mutable.HashMap[Int, Double]()
                tokens.foreach { term =>
                    if (vocab.contains(term)) {
                        val idx = vocab(term)
                        counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
                    }
                }
                (id, Vectors.sparse(vocab.size, counts.toSeq))
            }

        val topicSmoothing = 1.2
        val termSmoothing = 1.2

        // Set LDA parameters
        val lda = new LDA().setK(numTopics)
                           .setDocConcentration(topicSmoothing)
                           .setTopicConcentration(termSmoothing)
                           .setMaxIterations(numIterations)

        val ldaModel: DistributedLDAModel = lda.run(documents).asInstanceOf[DistributedLDAModel]

        val avgLogLikelihood = ldaModel.logLikelihood / documents.count()

        println("\nTweets: " + corpus.count)
        println("AvgLogLikelihood: " + avgLogLikelihood)
        println("Words: " + termCounts.map{case (word, count) => count}.reduce(_ + _) + "\n")

        // Print topics, showing top-weighted 10 terms for each topic.
        val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = numWordsByTopics)
        topicIndices.foreach { case (terms, termWeights) =>
            println("TOPIC:")
            terms.zip(termWeights).foreach { case (term, weight) =>
                println(s"${vocabArray(term.toInt)}\t\t$weight")
            }
            println()
        }
    }
}