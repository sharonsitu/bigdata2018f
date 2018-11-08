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

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String] (descr = "date", required = true)
  val text = opt[Boolean] (descr = "source files format: text")
  val parquet = opt[Boolean] (descr = "source files format: parquet")
  verify()
}

object Q2 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    if (args.text()) {
      log.info("Source files format: " + args.text())
    } else {
      log.info("Source files format: " + args.parquet())
    }

    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)

    /* select o_clerk, o_orderkey from lineitem, orders where
        l_orderkey = o_orderkey and
        l_shipdate = 'YYYY-MM-DD'
        order by o_orderkey asc limit 20; */
    val date = args.date()
    if (args.text()) {
      val lineitem = sc.textFile(args.input()+"/lineitem.tbl")
      val l = lineitem
        /* select those 1_shipdate = "YYYY-MM-DD" first */
        .flatMap(line => {
          val attributes = line.split("\\|")
          /* shipdate is the 11th attribute */
          if (attributes(10).contains(date)) {
            List((attributes(0).toInt,attributes(10)))
          } else {
            Nil
          }
        })
      val order = sc.textFile(args.input()+"/orders.tbl")
      val o = order
        .flatMap(line => {
          val attributes = line.split("\\|")
          /* clerk is the 7th attribute */
          List((attributes(0).toInt,attributes(6)))
        })
      val combine =   o
        /* outer join : (oderkey: (clerk,shipdate)) */
        .cogroup(l)
        .filter(_._2._1.nonEmpty)
        .filter(_._2._2.nonEmpty)
        .flatMap( p => {
          val dates = p._2._2.toList
          dates.map( date => (p._1,(p._2._1,date))).toList
        })
        .sortByKey(true,1)
        .take(20)
        .foreach(pair => {
          /* change CompactBuffer to String */
          val clerk = pair._2._1.map(x => x.toList.mkString)
          println(clerk.mkString,pair._1)
        })
    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val l = lineitemRDD
        /* select those 1_shipdate = "YYYY-MM-DD" first */
        .flatMap(attributes => {
          /* shipdate is the 11th attribute */
          if (attributes(10).toString.contains(date)) {
            List((attributes(0).toString.toInt,attributes(10).toString))
          } else {
            Nil
          }
        })
      val ordersDF = sparkSession.read.parquet(args.input()+"/orders")
      val ordersRDD = ordersDF.rdd
      val o = ordersRDD
        /* select those 1_shipdate = "YYYY-MM-DD" first */
        .flatMap(attributes => {
          /* clerk is the 7th attribute */
          List((attributes(0).toString.toInt,attributes(6).toString))
        })
      val combine =   o
        /* outer join : (oderkey: (clerk,shipdate)) */
        .cogroup(l)
        .filter(_._2._1.nonEmpty)
        .filter(_._2._2.nonEmpty)
        .flatMap( p => {
          val dates = p._2._2.toList
          dates.map( date => (p._1,(p._2._1,date))).toList
        })
        .sortByKey(true,1)
        .take(20)
        .foreach(pair => {
          /* change CompactBuffer to String */
          val clerk = pair._2._1.map(x => x.toList.mkString)
          println(clerk.mkString,pair._1)
        })
    }
  }
}
