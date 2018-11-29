/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

/** reference:
  https://github.com/eBay/Spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/StatefulNetworkWordCount.scala
  */

package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._
import org.apache.spark.streaming._

import scala.collection.mutable

class TrendingArrivalsConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())

  val goldman = List((-74.0141012, 40.7152191), (-74.013777, 40.7152275), (-74.0141027, 40.7138745), (-74.0144185, 40.7140753))
  val citigroup = List((-74.011869, 40.7217236), (-74.009867, 40.721493), (-74.010140,40.720053), (-74.012083, 40.720267))

  def WithinRegion(ranges: List[(Double,Double)], longitude: Double, latitude: Double) : Boolean = {
    var minLat = Double.MaxValue
    var maxLat = Double.MinValue
    var minLong = Double.MaxValue
    var maxLong = Double.MinValue
    for (range <- ranges) {
      var lon = range._1
      var lat = range._2
      if (minLat > lat) minLat = lat
      if (minLong > lon) minLong = lon
      if (maxLat < lat) maxLat = lat
      if (maxLong < lon) maxLong = lon
    }
    (longitude >= minLong) && (longitude <= maxLong) && (latitude >= minLat) && (latitude <= maxLat)
  }

  // Update the count of previous interval and current interval using mapWithState
  def mappingFunc(time: Time, key: String, one: Option[Int], state: State[Tuple3[Int, String, Int]]): Option[Tuple2[String,Tuple3[Int, String, Int]]] = {
    val cur = one.getOrElse(0).toInt
    var prev = 0
    if(state.exists()){
      prev = state.get()._1
    }
    val output = (key, (cur,"%08d".format(time.milliseconds),prev))
    state.update((cur,"%08d".format(time.milliseconds),prev))
    if ((cur >= 2 * prev) && (cur >= 10)) {
      if (key.equals("goldman")) {
        println(s"Number of arrivals to Goldman Sachs has doubled from ${prev.toString} to ${cur.toString} at ${"%08d".format(time.milliseconds)}!")
      }
      if (key.equals("citigroup")){
        println(s"Number of arrivals to Citigroup has doubled from ${prev.toString} to ${cur.toString} at ${"%08d".format(time.milliseconds)}!")
      }
    }
    Some(output)
  }

  def main(argv: Array[String]): Unit = {
    val args = new TrendingArrivalsConf(argv)
    val output = args.output()

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("TrendingArrivals")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 144)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    val wc = stream.map(_.split(","))
      .flatMap(tuple => {
        var lon = 0d
        var lat = 0d
        if (tuple(0) == "yellow") {
          lon = tuple(10).toDouble
          lat = tuple(11).toDouble
        } else {
          lon = tuple(8).toDouble
          lat = tuple(9).toDouble
        }
        if(WithinRegion(goldman,lon,lat)) {
          List(("goldman",1))
        } else if (WithinRegion(citigroup,lon,lat)) {
          List(("citigroup",1))
        } else {
          List()
        }
      })
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(10), Minutes(10))

    // Initial state RDD for mapWithState operation
    //val initialRDD = ssc.sparkContext.parallelize(List(("goldman", 0), ("citigroup", 0)))

    val stateDStream = wc
      .mapWithState(StateSpec.function(mappingFunc _))

    var streamedShot = stateDStream.stateSnapshots()
    streamedShot.foreachRDD(
      (rdd,TimeStamp) => {
        val time = "%08d".format(TimeStamp.milliseconds)
        rdd.saveAsTextFile(output+"/part-"+time)
      }
    )

    stateDStream.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}