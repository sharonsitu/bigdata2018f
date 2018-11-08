/* reference: Bespin WordCount.scala */

package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String] (descr = "date", required = true)
  val text = opt[Boolean] (descr = "source files format: text")
  val parquet = opt[Boolean] (descr = "source files format: parquet")
  verify()
}

object Q1 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  /*def wcIter(iter: Iterator[String]): Iterator[(String, Int)] = {
    val counts = new HashMap[String, Int]() { override def default(key: String) = 0 }

    iter.flatMap(line => tokenize(line))
      .foreach { t => counts.put(t, counts(t) + 1) }

    counts.iterator
  }*/

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    if (args.text()) {
      log.info("Source files format: " + args.text())
    } else {
      log.info("Source files format: " + args.parquet())
    }

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    /* select count(*) from lineitem where l_shipdate = 'YYYY-MM-DD' */
    val date = args.date()
    if (args.text()) {
      val textFile = sc.textFile(args.input()+"/lineitem.tbl")
      val count = textFile
        .flatMap(line => {
          val attributes = line.split("\\|")
          /* shipdate is the 11th attribute */
          if (attributes(10).contains(date)) {
            List("yes")
          } else {
            Nil
          }
        })
      println("ANSWER=" + count.count())
    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val count = lineitemRDD
        .flatMap(attributes => {
          /* shipdate is the 11th attribute */
          if (attributes(10).toString.contains(date)) {
            List("yes")
          } else {
            Nil
          }
        })
      println("ANSWER=" + count.count())
    }
  }
}
