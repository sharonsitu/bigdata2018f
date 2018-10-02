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
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
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
            val stripe : Map[String, Float]= Map();
            val prev = p.head
            val cur = p.tail.mkString
            //println("prev: "+prev+" cur: "+cur)
            stripe.put(cur,1.0f)
            (prev,stripe)
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
        val stripe: Map[String, Float] = Map();
        val stripes = values
        for ((k, v) <- stripes) {
          //println("key2: "+k+" value2:"+v)
          marginal = marginal + v
        }
        for ((k, v) <- stripes) {
          stripe.put(k, v / marginal)
        }
        (key, stripe)
      }
      }

    counts.saveAsTextFile(args.output())
  }
}
