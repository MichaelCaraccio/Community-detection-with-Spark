package MllibUtils

import scala.collection.mutable
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, DenseMatrix, Matrix, Vectors}
import scala.collection.mutable.ArrayBuffer
import Array._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors

class MllibUtils(_lda:LDA) {


    val RED = "\033[1;30m"
    val ENDC = "\033[0m"


    def color(str: String, col: String): String = "%s%s%s".format(col, str, ENDC)

    // http://stackoverflow.com/questions/2440134/is-this-the-proper-way-to-initialize-null-references-in-scala
    var lda:LDA = _lda
    //var ldaModel:DistributedLDAModel
   // private var ldaModel


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
    def findTopics(ldaModel:DistributedLDAModel, documents:RDD[(Long, Vector)], vocabArray: Array[String], numWordsByTopics:Int, displayResult:Boolean) : DistributedLDAModel= {

        println(color("\nCall findTopics", RED))

        //var lda2 = lda match { case Some(value) => value }

        //var ldaModel2 = ldaModel match { case Some(value) => value }

        //ldaModel = lda.run(documents).asInstanceOf[DistributedLDAModel]

        //ldaModel = Some(ldaModel2)
        //lda = Some(lda2)

        println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
        println("getDocConcentration: "+ lda.getDocConcentration)

        //ldaModel.topicDistributions.collect.foreach(println(_))

        //val avgLogLikelihood = ldaModel.logLikelihood / documents.count()

        // Print topics, showing top-weighted 10 terms for each topic.
        if(displayResult) {
            println("\nTweets: " + documents.count)
            //println("AvgLogLikelihood: " + avgLogLikelihood)


            val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = numWordsByTopics)
            topicIndices.foreach { case (terms, termWeights) =>
                println("TOPIC:")
                terms.zip(termWeights).foreach { case (term, weight) =>
                    println(s"${vocabArray(term.toInt)}\t\t$weight")
                }
                println()
            }
        }
        ldaModel
    }


        //println("-------------------------------------------------------")




    def createDocuments(corpus:RDD[String], numStopwords:Int): (RDD[(Long, Vector)], Array[String]) = {

        println(color("\nCall createDocuments", RED))

        // Split text into terms (words) and then remove :
        // -> (a) non-alphabetic terms
        // -> (b) short terms with < 4 characters
        // -> (c) the most common 20 terms (as “stopwords”)
        val tokenized: RDD[Seq[String]] =
            corpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))



        // Choose the vocabulary
        //   termCounts: Sorted list of (term, termCount) pairs
        val termCounts: Array[(String, Long)] = tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
        //println("Words: " + termCounts.map { case (word, count) => count }.reduce(_ + _) + "\n")


        //println("\nvocabArray\n")
        //termCounts.foreach(x => println("Word: "+x._1.toString + "   Count: " +x._2.toString))

        //   vocabArray: Chosen vocab (removing common terms)
        println("Avant:"+ termCounts.takeRight(termCounts.size - numStopwords).map(_._1).length)

        val vocabArray: Array[String] = termCounts.takeRight(termCounts.size - numStopwords).map(_._1)

        println("Après:" + vocabArray.length)
        vocabArray.foreach(println(_))
        //   vocab: Map term -> term index
        val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

        vocab.foreach(x => println("Word: "+x._1.toString + "   Count: " +x._2.toString))

        /*println("\ntokenized\n")
        tokenized.foreach(println(_))
        println("\nvocabArray\n")
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
                    println("TERM: "+ term + "\t\tid:" + id + "\t\tvocab.size: " + vocab.size +"\t\ttokens: " + tokens)

                }
                println("id: "+ id + "\t\tvocab.size:" + vocab.size + "\t\tcounts.toSeq: " + counts.toSeq)

                (id, Vectors.sparse(vocab.size, counts.toSeq))
            }
        (documents,vocabArray)
    }
}