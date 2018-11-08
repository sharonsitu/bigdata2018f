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

class Conf5(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val text = opt[Boolean] (descr = "source files format: text")
  val parquet = opt[Boolean] (descr = "source files format: parquet")
  verify()
}

object Q5 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf5(argv)

    log.info("Input: " + args.input())
    if (args.text()) {
      log.info("Source files format: " + args.text())
    } else {
      log.info("Source files format: " + args.parquet())
    }

    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

    if (args.text()) {
      val customer = sc.textFile(args.input()+"/customer.tbl")
      val c = customer
        .flatMap(line => {
          val attributes = line.split("\\|")
          val nationkey = attributes(3)
          // only take customers from Canada or USA
          if (nationkey == "3" || nationkey == "24") {
            List((attributes(0),attributes(3)))
          } else {
            List()
          }
        }).collectAsMap()
      val cb = sc.broadcast(c)

      val order = sc.textFile(args.input()+"/orders.tbl")
      val o = order
        .flatMap(line => {
          val attributes = line.split("\\|")
          val custkey = attributes(1)
          val nationkey = cb.value.getOrElse(custkey, "")
          if (nationkey != "") {
            List((attributes(0),nationkey))
          } else {
            List()
          }
        })

      val lineitem = sc.textFile(args.input()+"/lineitem.tbl")
      val l = lineitem
        .flatMap(line => {
          val attributes = line.split("\\|")
          List((attributes(0),attributes(10)))
        })

      val combine = l
        .cogroup(o)
        .filter(_._2._1.nonEmpty)
        .filter(_._2._2.nonEmpty)
        .flatMap(pair => {
          val orderkey = pair._1
          val dates = pair._2._1.toList
          val nation = pair._2._2.toList
          dates.map(date => ((nation(0).toInt,date.substring(0,7)),1)).toList
        })
        .reduceByKey(_ + _)
        .sortBy( p => {
          (p._1._1, p._1._2.substring(0,4).toInt, p._1._2.substring(5,7).toInt)
        })
        .collect()
        .foreach(pair => {
          println(pair._1._1,pair._1._2,pair._2)
        })

    } else {
      val sparkSession = SparkSession.builder.getOrCreate

      val customerDF = sparkSession.read.parquet(args.input()+"/customer")
      val customerRDD = customerDF.rdd
      val c = customerRDD
        .flatMap(attributes => {
          val nationkey = attributes(3).toString
          if (nationkey == "3" || nationkey == "24") {
            List((attributes(0).toString,attributes(3).toString))
          } else {
            List()
          }
        }).collectAsMap()
      val cb = sc.broadcast(c)

      val ordersDF = sparkSession.read.parquet(args.input()+"/orders")
      val ordersRDD = ordersDF.rdd
      val o = ordersRDD
        .flatMap(attributes => {
          val custkey = attributes(1).toString
          val nationkey = cb.value.getOrElse(custkey, "")
          if (nationkey != "") {
            List((attributes(0).toString,nationkey))
          } else {
            List()
          }
        })

      val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val l = lineitemRDD
        .flatMap(line => {
          List((line(0).toString, line(10).toString))
        })

      val combine = l
        .cogroup(o)
        .filter(_._2._1.nonEmpty)
        .filter(_._2._2.nonEmpty)
        .flatMap(pair => {
          val orderkey = pair._1
          val dates = pair._2._1.toList
          val nation = pair._2._2.toList
          dates.map(date => ((nation(0).toInt,date.substring(0,7)),1)).toList
        })
        .reduceByKey(_ + _)
        .sortBy( p => {
          (p._1._1, p._1._2.substring(0,4).toInt, p._1._2.substring(5,7).toInt)
        })
        .collect()
        .foreach(pair => {
          println(pair._1._1,pair._1._2,pair._2)
        })

    }
  }
}
