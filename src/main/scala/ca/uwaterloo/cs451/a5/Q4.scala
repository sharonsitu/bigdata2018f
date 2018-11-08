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

class Conf4(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String] (descr = "date", required = true)
  val text = opt[Boolean] (descr = "source files format: text")
  val parquet = opt[Boolean] (descr = "source files format: parquet")
  verify()
}

object Q4 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf4(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    if (args.text()) {
      log.info("Source files format: " + args.text())
    } else {
      log.info("Source files format: " + args.parquet())
    }

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)

    /* select n_nationkey, n_name, count(*) from lineitem, orders, customer, nation
          where l_orderkey = o_orderkey
          and o_custkey = c_custkey
          and c_nationkey = n_nationkey
          and l_shipdate = 'YYYY-MM-DD'
          group by n_nationkey, n_name
          order by n_nationkey asc; */
    val date = args.date()

    if (args.text()) {
      val nation = sc.textFile(args.input()+"/nation.tbl")
      val n = nation
        .flatMap(line => {
          val attributes = line.split("\\|")
          List((attributes(0),attributes(1)))
        }).collectAsMap()
      val nb = sc.broadcast(n)

      val customer = sc.textFile(args.input()+"/customer.tbl")
      val c = customer
        .flatMap(line => {
          val attributes = line.split("\\|")
          val nationkey = attributes(3)
          val nationname = nb.value.get(nationkey).get
          List((attributes(0),(nationkey,nationname)))
        }).collectAsMap()
      val cb = sc.broadcast(c)

      val order = sc.textFile(args.input()+"/orders.tbl")
      val o = order
        .flatMap(line => {
          val attributes = line.split("\\|")
          val custkey = attributes(1)
          val nationkey = cb.value.get(custkey).get._1
          val nationname = cb.value.get(custkey).get._2
          List((attributes(0),(nationkey,nationname)))
        })

      val lineitem = sc.textFile(args.input()+"/lineitem.tbl")
      val l = lineitem
        .flatMap(line => {
          val attributes = line.split("\\|")
          if (attributes(10).contains(date)) {
            List((attributes(0),attributes(10)))
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
          val dates = pair._2._1.toList
          val orderpair = pair._2._2.toList
          dates.map(pair => ((orderpair(0)._1.toInt,orderpair(0)._2),1)).toList
        })
        .reduceByKey(_ + _)
        .sortByKey(true,1)
        .collect()
        .foreach(pair => {
          val count = pair._2
          val key = pair._1._1
          val nationname = pair._1._2
          println(key,nationname,count)
        })
      nb.destroy()
      cb.destroy()
    } else {
      val sparkSession = SparkSession.builder.getOrCreate

      val nationDF = sparkSession.read.parquet(args.input()+"/nation")
      val nationRDD = nationDF.rdd
      val n = nationRDD
        .flatMap(attributes => {
          List((attributes(0).toString,attributes(1).toString))
        }).collectAsMap()
      val nb = sc.broadcast(n)

      val customerDF = sparkSession.read.parquet(args.input()+"/customer")
      val customerRDD = customerDF.rdd
      val c = customerRDD
        .flatMap(attributes => {
          val nationkey = attributes(3).toString
          val nationname = nb.value.get(nationkey).get
          List((attributes(0).toString,(nationkey,nationname)))
        }).collectAsMap()
      val cb = sc.broadcast(c)


      val ordersDF = sparkSession.read.parquet(args.input()+"/orders")
      val ordersRDD = ordersDF.rdd
      val o = ordersRDD
        .flatMap(attributes => {
          val custkey = attributes(1).toString
          val nationkey = cb.value.get(custkey).get._1
          val nationname = cb.value.get(custkey).get._2
          List((attributes(0).toString,(nationkey,nationname)))
        })


      val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val l = lineitemRDD
        .flatMap(attributes => {
          if (attributes(10).toString.contains(date)) {
            List((attributes(0).toString,attributes(10).toString))
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
          val dates = pair._2._1.toList
          val orderpair = pair._2._2.toList
          dates.map(pair => ((orderpair(0)._1.toInt,orderpair(0)._2),1)).toList
        })
        .reduceByKey(_ + _)
        .sortByKey(true,1)
        .collect()
        .foreach(pair => {
          val count = pair._2
          val key = pair._1._1
          val nationname = pair._1._2
          println(key,nationname,count)
        })

      nb.destroy()
      cb.destroy()
    }
  }
}
