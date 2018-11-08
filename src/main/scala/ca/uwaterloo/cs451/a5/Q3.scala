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

class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String] (descr = "date", required = true)
  val text = opt[Boolean] (descr = "source files format: text")
  val parquet = opt[Boolean] (descr = "source files format: parquet")
  verify()
}

object Q3 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf3(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    if (args.text()) {
      log.info("Source files format: " + args.text())
    } else {
      log.info("Source files format: " + args.parquet())
    }

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)

    /* select l_orderkey, p_name, s_name from lineitem, part, supplier
       where
       l_partkey = p_partkey and
       l_suppkey = s_suppkey and
       l_shipdate = 'YYYY-MM-DD'
       order by l_orderkey asc limit 20 */
    val date = args.date()
    if (args.text()) {
      val part = sc.textFile(args.input()+"/part.tbl")
      val p = part
        .flatMap(line => {
          val attributes = line.split("\\|")
          /* name is the 2nd attribute */
          List((attributes(0),attributes(1)))
        }).collectAsMap()
      val supplier = sc.textFile(args.input()+"/supplier.tbl")
      val s = supplier
        .flatMap(line => {
          val attributes = line.split("\\|")
          /* name is the 2nd attribute */
          List((attributes(0),attributes(1)))
        }).collectAsMap()

      val pb = sc.broadcast(p)
      val sb = sc.broadcast(s)

      val lineitem = sc.textFile(args.input()+"/lineitem.tbl")
      val l = lineitem
        /* select those 1_shipdate = "YYYY-MM-DD" first */
        .flatMap(line => {
          val attributes = line.split("\\|")
          /* shipdate is the 11th attribute */
          if (attributes(10).contains(date)) {
            /* partkey is the 2nd attribute, suppkey is the 3rd attribute */
            val partname = pb.value.get(attributes(1))
            val suppliername = sb.value.get(attributes(2))
            if ( ! partname.isEmpty && ! suppliername.isEmpty) {
              List((attributes(0).toInt,(partname.get,suppliername.get)))
            } else {
              Nil
            }
          } else {
            Nil
          }
        })
        .sortByKey(true,1)
        .take(20)
        .foreach(pair => {
          println(pair._1,pair._2._1,pair._2._2)
        })
      
    } else {
      val sparkSession = SparkSession.builder.getOrCreate

      val partDF = sparkSession.read.parquet(args.input()+"/part")
      val partRDD = partDF.rdd
      val p = partRDD
        .flatMap(attributes => {
          List((attributes(0).toString,attributes(1).toString))
        }).collectAsMap()

      val supplierDF = sparkSession.read.parquet(args.input()+"/supplier")
      val supplierRDD = supplierDF.rdd
      val s = supplierRDD
        .flatMap(attributes => {
          List((attributes(0).toString,attributes(1).toString))
        }).collectAsMap()

      val pb = sc.broadcast(p)
      val sb = sc.broadcast(s)

      val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val l = lineitemRDD
        .flatMap(attributes => {
          if (attributes(10).toString.contains(date)) {
            /* partkey is the 2nd attribute, suppkey is the 3rd attribute */
            val partname = pb.value.get(attributes(1).toString)
            val suppliername = sb.value.get(attributes(2).toString)
            if ( ! partname.isEmpty && ! suppliername.isEmpty) {
              List((attributes(0).toString.toInt,(partname.get,suppliername.get)))
            } else {
              Nil
            }
          } else {
            Nil
          }
        })
        .sortByKey()
        .take(20)
        .foreach(pair => {
          println(pair._1,pair._2._1,pair._2._2)
        })
    }
  }
}
