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

class Conf6(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String] (descr = "date", required = true)
  val text = opt[Boolean] (descr = "source files format: text")
  val parquet = opt[Boolean] (descr = "source files format: parquet")
  verify()
}

object Q6 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf6(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    if (args.text()) {
      log.info("Source files format: " + args.text())
    } else {
      log.info("Source files format: " + args.parquet())
    }

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)

    /* select count(*) from lineitem where l_shipdate = 'YYYY-MM-DD' */
    val date = args.date()
    if (args.text()) {
      val textFile = sc.textFile(args.input()+"/lineitem.tbl")
      val count = textFile
        .flatMap(line => {
          val a = line.split("\\|")
          if (a(10).contains(date)) {
            val flag = a(8)
            val statu = a(9)
            val qty = a(4).toDouble
            val extendedprice = a(5).toDouble
            val discount = a(6).toDouble
            val tax = a(7).toDouble
            val disc_price = extendedprice*(1-discount)
            val charge = disc_price*(1+tax)
            List(((flag,statu),(qty,extendedprice,disc_price,charge,discount,1)))
          } else {
            Nil
          }
        })
        .reduceByKey((p1,p2) => {
          (p1._1+p2._1,p1._2+p2._2,p1._3+p2._3,p1._4+p2._4,p1._5+p2._5,p1._6+p2._6)
        }).collect()
        .foreach(p => {
          val values = p._2
          val avg_qty = values._1/values._6
          val avg_price = values._2/values._6
          val avg_disc = values._5/values._6
          println((p._1._1,p._1._2,values._1,values._2,values._3,values._4,avg_qty,avg_price,avg_disc,values._6))
        })
    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val count = lineitemRDD
        .flatMap(a => {
          /* shipdate is the 11th attribute */
          if (a(10).toString.contains(date)) {
              val flag = a(8).toString
              val statu = a(9).toString
              val qty = a(4).toString.toDouble
              val extendedprice = a(5).toString.toDouble
              val discount = a(6).toString.toDouble
              val tax = a(7).toString.toDouble
              val disc_price = extendedprice * (1 - discount)
              val charge = disc_price * (1 + tax)
              List(((flag, statu), (qty, extendedprice, disc_price, charge, discount, 1)))
          } else {
            Nil
          }
        })
        .reduceByKey((p1,p2) => {
          (p1._1+p2._1,p1._2+p2._2,p1._3+p2._3,p1._4+p2._4,p1._5+p2._5,p1._6+p2._6)
        }).collect()
        .foreach(p => {
          val values = p._2
          val avg_qty = values._1/values._6
          val avg_price = values._2/values._6
          val avg_disc = values._5/values._6
          println((p._1._1,p._1._2,values._1,values._2,values._3,values._4,avg_qty,avg_price,avg_disc,values._6))
        })
    }
  }
}
