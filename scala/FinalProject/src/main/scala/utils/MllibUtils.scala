package utils

import org.apache.spark.mllib.clustering._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * Topic models automatically infer the topics discussed in a collection of documents. These topics can be used
 * to summarize and organize documents, or used for featurization and dimensionality reduction in later stages
 * of a Machine Learning (ML) pipeline.
 *
 * LDA is not given topics, so it must infer them from raw text. LDA defines a topic as a distribution over words.
 */
class MllibUtils(/*_dictionnary: ArrayBuffer[String], _currentTweet: ArrayBuffer[String])*/) {

    // Text Color
    val RED = "\033[1;30m"
    val ENDC = "\033[0m"

    // LDA attributs
    //var dictionnary: ArrayBuffer[String] = _dictionnary
    //var currentTweet: ArrayBuffer[String] = _currentTweet
    //var currentTweetRDD: RDD[String] = _sc.parallelize(_dictionnary)
    //var sc: SparkContext = _sc

    /**
     * @constructor newTweet
     *
     *              Set currentTweet attribut and add the new tweet to the dictionnary
     *
     * @param String $newTweet - tweet content
     *
     * @return Unit
     */
    def newTweet(newTweet: String, newTweetRDD: RDD[String]): Unit = {

        // Delete old currentTweet
        //currentTweet = new ArrayBuffer[String]()

        // Set new value
        //currentTweet += newTweet

        // Convert it to RDD
        //currentTweetRDD = newTweetRDD

        // Add tweet to dictionnary
        //addToDictionnary(newTweet)

        //currentTweetRDD.collect().foreach(println(_))
    }

    def newTweet(newTweet: String): Unit = {

        // Delete old currentTweet
        //currentTweet = new ArrayBuffer[String]()

        // Set new value
        //currentTweet += newTweet

        // Convert it to RDD
        //currentTweetRDD = sc.parallelize(currentTweet)

        // Add tweet to dictionnary
        //addToDictionnary(newTweet)

        //currentTweetRDD.collect().foreach(println(_))
    }

    /**
     * @constructor createDocuments
     *
     *              Set currentTweet attribut and add the new tweet to the dictionnary
     *
     * @param SparkContext $sc - LDA Model (LocalModel)
     * @param Int $numStopwords - Contains all distinct words set to LDA
     *
     * @return RDD[(Long, Vector)] and Array[String] : documentsRDD and array of vocabulary
     */
    /*def createDocuments(sc: SparkContext, numStopwords: Int): (RDD[(Long, Vector)], Array[String]) = {

        println(color("\nCall createDocuments", RED))

        val corpus: RDD[String] = sc.parallelize(dictionnary)

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
        val vocabArray: Array[String] = termCounts.takeRight(termCounts.length - numStopwords).map(_._1)


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
                counts(idx) = counts.getOrElse(idx, 0.0) + tokenizedTweet.collect().flatten.count(_ == tokens)

                // Return word ID and Vector
                (id.toLong, Vectors.sparse(vocab.size, counts.toSeq))
            }

        // Transform it to RDD
        val documentsRDD = sc.parallelize(documents.toSeq)

        // Display RDD
        documentsRDD.collect().foreach(println(_))

        // Return
        (documentsRDD, vocabArray)
    }*/

    /**
     * @constructor addToDictionnary
     *
     *              Add tweet content to the dictionnary. A dictionnary contains every words set to the LDA
     *
     * @param String $newTweet - tweet content
     *
     * @return Unit
     */
    /*def addToDictionnary(newTweet: String): Unit = {
        dictionnary += newTweet
    }*/

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
        if (displayResult) {
            topicIndices.foreach { case (terms, termWeights) =>

                if(displayResult)
                    println("TOPICS:")

                var tabTopics = new ArrayBuffer[String]()

                terms.zip(termWeights).foreach { case (term, weight) =>

                    if(displayResult)
                        println(s"${vocabArray(term.toInt)}\t\t$weight")

                    tabTopics += vocabArray(term.toInt)
                }

                seqC = seqC :+(T, SG.toString, it.toString, tabTopics.mkString(";"))

                println("T: " + T + " SG: " + SG + "TopicN: " + it + " c: " + tabTopics.mkString(";"))
                it += 1

                if(displayResult)
                    println()
            }
        }
        seqC.toSeq
    }

    /**
     * @constructor createDocuments
     *
     *              Set currentTweet attribut and add the new tweet to the dictionnary
     *
     * @param SparkContext $sc - LDA Model (LocalModel)
     * @param Int $numStopwords - Contains all distinct words set to LDA
     *
     * @return RDD[(Long, Vector)] and Array[String] : documentsRDD and array of vocabulary
     */
    /*def createDocuments(corpus: RDD[String], numStopwords: Int): (Seq[(Long, Vector)], Array[String]) = {

        println(color("\nCall createDocuments", RED))

        //val corpus: RDD[String] = sc.parallelize(dictionnary)

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
        //val documentsRDD = sc.parallelize(documents.toSeq)

        // Display RDD
        //documentsRDD.collect.foreach(println(_))

        // Return
        (documents.toSeq, vocabArray)
    }*/


    /**
     * @constructor createDocuments
     *
     *              Set currentTweet attribut and add the new tweet to the dictionnary
     *
     * @param SparkContext $sc - LDA Model (LocalModel)
     * @param Int $numStopwords - Contains all distinct words set to LDA
     *
     * @return RDD[(Long, Vector)] and Array[String] : documentsRDD and array of vocabulary
     */
    /* def createDocumentsCON(totalcorpus : RDD[String], numStopwords: Int): (Seq[(Long, Vector)], Array[String]) = {

         println(color("\nCall createDocuments", RED))

         // Split every tweets's text into terms (words) and then remove :
         // -> (a) non-alphabetic terms
         // -> (b) short terms with < 4 characters
         // -> (c) to lower
         val tokenizedCorpus: RDD[Seq[String]] =
             totalcorpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))


         for(corpus <- totalcorpus){

             // Split tweet's text into terms (words) and then remove :
             // -> (a) non-alphabetic terms
             // -> (b) short terms with < 4 characters
             // -> (c) to lower
             //val tokenizedTweet =
             //    corpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))
             val tokenizedTweet = corpus.toLowerCase.split("\\s").filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter))

             // Choose the vocabulary
             //   termCounts: Sorted list of (term, termCount) pairs
             val termCounts: Array[(String, Long)] = tokenizedCorpus.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)

             // vocabArray contains all distinct words
             val vocabArray: Array[String] = termCounts.takeRight(termCounts.length - numStopwords).map(_._1)


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
                     counts(idx) = counts.getOrElse(idx, 0.0) + tokenizedTweet.flatten.count(_ == tokens)

                     // Return word ID and Vector
                     (id.toLong, Vectors.sparse(vocab.size, counts.toSeq))
                 }

             // Transform it to RDD
             //val documentsRDD = sc.parallelize(documents.toSeq)

             // Display RDD
             //documentsRDD.collect().foreach(println(_))

             // Return
             (documents.toSeq, vocabArray)
         }


     }*/

    def color(str: String, col: String): String = "%s%s%s".format(col, str, ENDC)

    /*def getDictionnary: ArrayBuffer[String] ={
        dictionnary
    }*/

    /* def createAllDocs(con:DataFrame): Unit ={
         println("suce")
         val filsdepute = new ArrayBuffer[(RDD[(Long, Vector)], Array[String])]
         println("sucsse")
         for(result <- con) {
             //result.getString(0)
         //}
         //for(c <- con){
             newTweet(result.getString(0))
             //var corpus: RDD[String] = sc.parallelize(dictionnary)

             filsdepute += createDocuments(corpus,0)

        }

         for(fils <- filsdepute){
             var ldaModel = lda.run(fils._1)
             ldaModel = findTopics(ldaModel, fils._2, 5, true)
         }
     }*/

}