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

class ApplySpamConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String] (descr = "model", required = true)
  verify()
}

object ApplySpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ApplySpamConf(argv)
    val input = args.input()
    val output = args.output()
    val model = args.model()

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(output)
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val spamclassifier = sc.textFile(model)
      .map(line => {
        val tokens = line.split(",")
        val feature = tokens(0).substring(1,tokens(0).length)
        val weight = tokens(1).substring(0,tokens(1).length-1)
        (feature.toInt,weight.toDouble)
      })
      .collectAsMap()

    val scbc = sc.broadcast(spamclassifier)

    val textFile = sc.textFile(input)
      .map(line => {
        val tokens = line.split("\\s+")
        val docid = tokens(0)
        val isSpam = tokens(1)
        val w = scbc.value
        // Scores a document based on its list of features.
        var score = 0d
        val features = tokens.slice(2,tokens.length).map(_.toInt)
          .foreach(f => if (w.contains(f)) score += w.get(f).get)
        val spamOrham = if (score > 0) "spam" else "ham"
        (docid,isSpam,score,spamOrham)
      })

    textFile.saveAsTextFile(output)
  }
}