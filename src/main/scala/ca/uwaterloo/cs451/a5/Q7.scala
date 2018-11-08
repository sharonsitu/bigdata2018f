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

class Conf7(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String] (descr = "date", required = true)
  val text = opt[Boolean] (descr = "source files format: text")
  val parquet = opt[Boolean] (descr = "source files format: parquet")
  verify()
}

object Q7 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf7(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    if (args.text()) {
      log.info("Source files format: " + args.text())
    } else {
      log.info("Source files format: " + args.parquet())
    }

    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)

    val date = args.date()

    if (args.text()) {
      val lineitem = sc.textFile(args.input()+"/lineitem.tbl")
      val l = lineitem
        .flatMap(line => {
          val attributes = line.split("\\|")
          val shipdate = attributes(10)
          if (shipdate > date) {
            val extendedprice = attributes(5).toDouble
            val discount = attributes(6).toDouble
            val revenue = extendedprice*(1-discount)
            List((attributes(0),revenue))
          } else {
            Nil
          }
        })

      val customer = sc.textFile(args.input()+"/customer.tbl")
      val c = customer
        .flatMap(line => {
          val attributes = line.split("\\|")
          List((attributes(0),attributes(1)))
        }).collectAsMap()
      val cb = sc.broadcast(c)

      val order = sc.textFile(args.input()+"/orders.tbl")
      val o = order
        .flatMap(line => {
          val attributes = line.split("\\|")
          val orderday = attributes(4)
          if (orderday < date) {
            val custkey = attributes(1)
            val custname = cb.value.get(custkey).get
            val pri = attributes(7)
            List((attributes(0),(orderday,pri,custname)))
          } else {
            Nil
          }
        })

      val combine = l
        .cogroup(o)
        .filter(_._2._1.nonEmpty)
        .filter(_._2._2.nonEmpty)
        .flatMap(pair => {
          val orderkey = pair._1
          val values = pair._2
          val revenues = values._1.toList
          val orderday = values._2.toList(0)._1
          val pri = values._2.toList(0)._2
          val custname = values._2.toList(0)._3
          revenues.map(r => ((custname, orderkey, orderday, pri), r.toDouble)).toList
        })
        .reduceByKey(_ + _)
        .sortBy(_._2,false)
        .take(10)
        .foreach(p => {
          println(p._1._1,p._1._2,p._2,p._1._3,p._1._4)
        })
      cb.destroy()
    } else {
      val sparkSession = SparkSession.builder.getOrCreate

      val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val l = lineitemRDD
        .flatMap(attributes => {
          val shipdate = attributes(10).toString
          if (shipdate > date) {
            val extendedprice = attributes(5).toString.toDouble
            val discount = attributes(6).toString.toDouble
            val revenue = extendedprice*(1-discount)
            List((attributes(0).toString,revenue))
          } else {
            Nil
          }
        })

      val customerDF = sparkSession.read.parquet(args.input()+"/customer")
      val customerRDD = customerDF.rdd
      val c = customerRDD
        .flatMap(attributes => {
          List((attributes(0).toString,attributes(1).toString))
        }).collectAsMap()
      val cb = sc.broadcast(c)


      val ordersDF = sparkSession.read.parquet(args.input()+"/orders")
      val ordersRDD = ordersDF.rdd
      val o = ordersRDD
        .flatMap(attributes => {
          val orderday = attributes(4).toString
          if (orderday < date) {
            val custkey = attributes(1).toString
            val custname = cb.value.get(custkey).get
            val pri = attributes(7).toString
            List((attributes(0).toString,(orderday,pri,custname)))
          } else {
            Nil
          }
        })

      val combine = l
        .cogroup(o)
        .filter(_._2._1.nonEmpty)
        .filter(_._2._2.nonEmpty)
        .flatMap(pair => {
          val orderkey = pair._1
          val values = pair._2
          val revenues = values._1.toList
          val orderday = values._2.toList(0)._1
          val pri = values._2.toList(0)._2
          val custname = values._2.toList(0)._3
          revenues.map(r => ((custname, orderkey, orderday, pri), r.toDouble)).toList
        })
        .reduceByKey(_ + _)
        .sortBy(_._2,false)
        .take(10)
        .foreach(p => {
          println(p._1._1,p._1._2,p._2,p._1._3,p._1._4)
        })
      cb.destroy()
    }
  }
}
