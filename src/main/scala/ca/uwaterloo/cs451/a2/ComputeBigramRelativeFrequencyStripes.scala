package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner
import scala.collection.mutable._


class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, numexecutors, executorcores)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(8))
  val numexecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorcores = opt[Int](descr = "number of cores", required = false, default = Some(1))
  verify()
}

class MyPartitioner2(numReduceTasks: Int) extends Partitioner {
  def numPartitions: Int = numReduceTasks

  def getPartition(key: Any): Int = key match {
    case (x: String, y: String) => (x.hashCode & Int.MaxValue) % numReduceTasks
  }
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Number of executors: " + args.numexecutors())
    log.info("Number of cores: " + args.executorcores())

    val conf = new SparkConf().setAppName("ComputeBigramRelativeFrequencyStripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(),args.reducers())

    val counts = textFile
      // each pair will be set as (A,(B,1))
      .flatMap(line => {
        val tokens = tokenize(line)
        // split the words(A,B)
        if (tokens.length > 1) {
          tokens.sliding(2).map(p => {
            val prev = p.head
            val cur = p.tail.mkString
            //println("prev: "+prev+" cur: "+cur)
            (prev,Map(cur->1.0f))
          })
        } else {
          List()
        }
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
      // update pairs (A,(B,count)) to (A,(B,frequency))
      .map {
      case (key, values) => {
        var marginal = 0.0f
        marginal = values.values.foldLeft(0.0f){(a,b) => a + b}
        var stripe = values.map {
          case (k,v) => {
            (k,v/marginal)
          }
        }
        (key,stripe)
      }
      }

    counts.saveAsTextFile(args.output())
  }
}
