/* reference: Bespin WordCount.scala */

package ca.uwaterloo.cs451.a6

import io.bespin.scala.util.Tokenizer

import collection.mutable.HashMap


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession
import scala.util.Random

class TrainSpamConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String] (descr = "model", required = true)
  val shuffle = opt[Boolean](descr = "shuffle")
  verify()
}

object TrainSpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new TrainSpamConf(argv)
    val input = args.input()
    val model = args.model()
    val shuffle = args.shuffle()

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    FileSystem.get(sc.hadoopConfiguration).delete(new Path(model), true)

    // w is the weight vector (make sure the variable is within scope)
    val w = HashMap[Int, Double]()

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]): Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    // This is the main learner:
    val delta = 0.002

    // For each instance...
    var textFile = sc.textFile(input)

    if (shuffle) {
      textFile = textFile
          .map(line => {
            //  generate a random number for each instance
            var random = Random.nextInt()
            (random,line)
          })
          .sortByKey()
          // get the data again
          .map(_._2)
    }

    val trained = textFile
      .map(line =>{
        val tokens = line.split("\\s+")
        // Parse input
        val docid = tokens(0)
        val isSpam = if (tokens(1) == "spam") 1 else 0
        // label
        val features = tokens.slice(2,tokens.length).map(_.toInt)
        // feature vector of the training instance
        (0, (docid, isSpam, features))
      })
      .groupByKey(1)
      // Update the weights as follows:
      .flatMap(input => {
      input._2.foreach(tuple => {
        val isSpam = tuple._2
        val features = tuple._3
        val score = spamminess(features)
        val prob = 1.0 / (1 + math.exp(-score))
        features.foreach(f => {
          if (w.contains(f)) {
            w(f) += (isSpam - prob) * delta
          } else {
            w(f) = (isSpam - prob) * delta
          }
        })
      })
      w
    })

    trained.saveAsTextFile(model)
  }
}