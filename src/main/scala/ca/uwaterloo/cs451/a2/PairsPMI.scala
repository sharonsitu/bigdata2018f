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

class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "number of threshold", required = false, default = Some(1))
  verify()
}

class MyPartitioner3(numReduceTasks: Int) extends Partitioner {
  def numPartitions: Int = numReduceTasks

  def getPartition(key: Any): Int = key match {
    case (x: String, y: String) => (x.hashCode & Int.MaxValue) % numReduceTasks
  }
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf3(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("PairsPMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    val threshold = args.threshold()

    // for single word
    val counts = textFile
      .flatMap(line => {
        var tokens = tokenize(line)
        // get 40 words of the line
        tokens = tokens.take(min(40,tokens.size))
        if (tokens.length >= 2) {
          var singleword : Map[String,String] = Map()
          // add distinct words in line to singleword
          tokens.map { p => {
            if (!singleword.contains(p)) {
              singleword.put(p, "*")
              //println("single "+p)
            }
          }}
          singleword.put("totalline","*")
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

    // for pairs
      val pairs = textFile
        .flatMap(line => {
          var tokens = tokenize(line)
          // get 40 words of the line
          tokens = tokens.take(min(40,tokens.size))
          if (tokens.length >= 2) {
            var singleword : Map[String,String] = Map()
            // add distinct words in line to singleword
            tokens.map { p => {
              if (! singleword.contains(p)) {
                singleword.put(p,"*")
                //println("single "+p)
              }
            }}
            var wordpairs = new ListBuffer[(String,String)]()
            // add distinct pairs in line to wordpairs
            for ((k,v) <- singleword) {
              var key = k
              for ((k,v) <- singleword) {
                if (key != k) {
                  //println("pair: "+key+" "+k)
                  wordpairs += ((key,k))
                }
              }
            }
            wordpairs.toList
          } else {
            List()
          }
        })
        .map(pair => {
          //println(pair._1+" "+pair._2)
          (pair,1.0f)
        })
        .reduceByKey(_ + _)
        // Repartition the RDD according to the given partitioner and, within each resulting partition, sort records by their keys
        .repartitionAndSortWithinPartitions(new MyPartitioner3(args.reducers()))
        .filter{ case ((w1, w2), value) => value.toInt >= threshold }
        // calculate PMI
      .map{ case ((w1, w2), value) => {
        val countXY = value
        val totalline : Float = countsSingleWord.value.get("totalline","*").get
        val countX : Float = countsSingleWord.value.get(w1,"*").get
        val countY : Float = countsSingleWord.value.get(w2,"*").get
        //println("T "+totalline)
        //println(w1+countX)
        //println(w2+countY)
        val probXY : Float = (countXY / totalline)
        val probX : Float = (countX / totalline)
        val probY : Float = (countY / totalline)
        val pmiF = log10(countXY / ((countX * countY) / totalline))
        ((w1,w2),(pmiF,value))
      }}


    pairs.saveAsTextFile(args.output())
  }
}
