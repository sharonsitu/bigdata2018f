package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

class MyPartitioner(numReduceTasks: Int) extends Partitioner {
  def numPartitions: Int = numReduceTasks

  def getPartition(key: Any): Int = key match {
    case (x: String, y: String) => (x.hashCode & Int.MaxValue) % numReduceTasks
  }
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("ComputeBigramRelativeFrequencyPairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(),args.reducers())

    var marginal = 0.0f

    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        // split the words(A,B)
        if (tokens.length >= 2) {
          var pairs = tokens.sliding(2).map(p => (p.head,p.tail.mkString)).toList
          var singleword = tokens.init.map(w => (w, "*")).toList
          var bigram = pairs ++ singleword
          bigram
        } else {
          List()
        }
      })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
      // Repartition the RDD according to the given partitioner and, within each resulting partition, sort records by their keys
      .repartitionAndSortWithinPartitions(new MyPartitioner(args.reducers()))
      .map(bigram => bigram._1 match {
        case (_,"*") => {
          marginal = bigram._2
          (bigram._1,bigram._2)
        }
        case (_,_) => (bigram._1,bigram._2/marginal)
      })

    counts.saveAsTextFile(args.output())
  }
}
