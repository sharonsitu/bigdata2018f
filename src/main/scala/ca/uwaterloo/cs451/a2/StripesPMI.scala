package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner
import scala.math._
import scala.collection.mutable._
import scala.collection.JavaConverters._

class Conf4(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, numexecutors, executorcores)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "number of threshold", required = false, default = Some(8))
  val numexecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorcores = opt[Int](descr = "number of cores", required = false, default = Some(1))
  verify()
}

object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf4(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())
    log.info("Number of executors: " + args.numexecutors())
    log.info("Number of cores: " + args.executorcores())

    val conf = new SparkConf().setAppName("StripesPMI")
    val sc = new SparkContext(conf)
    val threshold = args.threshold()

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), args.reducers())

    val totalline = textFile.count()

    val counts = textFile
      .flatMap(line => {
        var tokens = tokenize(line)
        // get 40 words of the line
        tokens = tokens.take(min(40,tokens.length))
        if (tokens.length >= 2) {
          var singleword : Map[String,String] = Map()
          // add distinct words in line to singleword
          tokens.map { p => {
            if (!singleword.contains(p)) {
              singleword.put(p, "*")
              //println("single "+p)
            }
          }}
          singleword
        } else {
          List()
        }
      })
      .map(word => {
        //println(word._1+" "+word._2)
        (word,1.0f)
      })
      .reduceByKey(_ + _)

    // get result for the first "JOB"
    val countsSingleWord = sc.broadcast(counts.collectAsMap())

    textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val words = tokens.take(Math.min(tokens.length, 40)).distinct
        if (words.length > 1) {
          // add distinct pairs
          var stripes : ListBuffer[(String, Map[String, Float])] = ListBuffer()
          for (i <- 0 until words.length) {
            var wordmap : Map[String,Float] = Map()
            var prev = words(i)
            for (j <- 0 until words.length) {
              var cur = words(j)
              if ((i != j) && (prev != cur)) {
                wordmap.put(cur,1.0f)
              }
            }
            stripes += ((prev,wordmap))
          }
          stripes.toList
        } else List()
      })
      // for pairs (A,(B,count)), update the value of count
      .reduceByKey((m1,m2) => {
      var m = m1 ++ m2.map{
        case (k,v) => {
          var value = v + m1.getOrElse(k,0.0f)
          //println("key: "+k+" value: "+ value)
          (k -> value)
        }
      }
      m
    })

      // update pairs (A,(B,count)) to (A,(B,(PMI,count))
      .map {
      case (key, values) => {
        var stripe : Map[String,Pair[Float,Float]] = Map()
        values.map {
          case (k,v) => {
            if (v.toInt >= threshold) {
              val countX : Float = countsSingleWord.value.get(key,"*").get
              val countY : Float = countsSingleWord.value.get(k,"*").get
              val countXY : Float = v
              val probXY : Float = (countXY / totalline)
              val probX : Float = (countX / totalline)
              val probY : Float = (countY / totalline)
              val pmi = log10(probXY / (probX * probY)).toFloat
              val pmicount = new Pair[Float, Float](pmi, v)
              //println("hello")
              stripe.put(k,pmicount)
            }
          }
        }
        (key,stripe)
      }
      }

      .saveAsTextFile(args.output())
  }
}
