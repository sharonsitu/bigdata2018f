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

class ApplyEnsembleSpamConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String] (descr = "model", required = true)
  val method = opt[String] (descr = "method", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ApplyEnsembleSpamConf(argv)
    val input = args.input()
    val output = args.output()
    val model = args.model()
    val method = args.method()

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())
    log.info("Method: " + args.method())

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(output)
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val sources = List("/part-00000","/part-00001","/part-00002")
    val spamclassifier =  sources
      .map (source => {
        sc.textFile(model + source)
          .map(line => {
            val tokens = line.split(",")
            val feature = tokens(0).substring(1, tokens(0).length)
            val weight = tokens(1).substring(0, tokens(1).length - 1)
            (feature.toInt, weight.toDouble)
          })
          .collectAsMap()
      })

    val scbc = sc.broadcast(spamclassifier)

    val textFile = sc.textFile(input)
      .map(line => {
        val tokens = line.split("\\s+")
        val docid = tokens(0)
        val isSpam = tokens(1)
        val w = scbc.value
        // Scores a document based on its list of features.
        var curscore = 0d
        val features = (0 to 2).map (
          index => {
            var s = 0d
            tokens.slice(2, tokens.length).map(_.toInt)
              .foreach(f => if (w(index).contains(f)) s += w(index).get(f).get)
            curscore += s
            s
          })
        val score = if (method == "average") {
          curscore / 3
        } else {
          var vote = 0
          features.map(
            score => {
              if (score > 0) vote += 1 else vote -= 1
            }
          )
          vote
        }
        val spamOrham = if (score > 0) "spam" else "ham"
        (docid, isSpam, score, spamOrham)
      })

    textFile.saveAsTextFile(output)
  }
}