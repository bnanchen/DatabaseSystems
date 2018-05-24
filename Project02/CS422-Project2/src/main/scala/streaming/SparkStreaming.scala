package streaming;

import scala.io.Source
import scala.util.control._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming._

import scala.collection.Seq
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.Strategy
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.net.URI

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.sketch.CountMinSketch

import scala.util.hashing.MurmurHash3._

class SparkStreaming(sparkConf: SparkConf, args: Array[String]) {

  val sparkconf = sparkConf;

  // get the directory in which the stream is filled.
  val inputDirectory = args(0)

  // number of seconds per window
  val seconds = args(1).toInt;

  // K: number of heavy hitters stored
  val TOPK = args(2).toInt;

  // precise Or approx
  val execType = args(3);

  // only used by "approx" method
  var epsilon: Double = 0.0

  // only used by "approx" method
  var delta: Double = 0.0

  if (execType.contains("approx")) {
    // only used by "approx" method
    epsilon = args(4).toDouble

    // only used by "approx" method
    delta= args(5).toDouble
  }

  //  create a StreamingContext, the main entry point for all streaming functionality.
  val ssc = new StreamingContext(sparkConf, Seconds(seconds));

  def consume() {

    // create a DStream that represents streaming data from a directory source.
    val linesDStream: DStream[String] = ssc.textFileStream(inputDirectory);

    // parse the stream. (line -> (IP1, IP2))
    val words: DStream[(String, String)] = linesDStream.map(x => (x.split("\t")(0), x.split("\t")(1)))

    if (execType.contains("precise")) {
      var global: Map[(String, String), Int] = Map()

      words.foreachRDD{rdd =>
        val currentBatch: Map[(String, String), Int] = rdd.map{ips => (ips, 1)}.reduceByKey(_+_).collect().toMap

        if (currentBatch.nonEmpty)
          global = (global.toSeq ++ currentBatch.toSeq).groupBy(_._1).mapValues(_.map(_._2).sum)

        val batchTopK: List[((String, String), Int)] = currentBatch.toList.sortWith{_._2 > _._2}.take(TOPK)
        val globalTopK: List[((String, String), Int)] = global.toList.sortWith{_._2 > _._2}.take(TOPK)
        val batchToPrint: String = "This batch: ["+ batchTopK.map(p => "( "+ p._2 +",("+ p._1._1 +", "+ p._1._2 +"))").fold("")(_+_) + "]"
        val globalToPrint: String = "Global :["+ globalTopK.map(p => "( "+ p._2 + ",("+ p._1._1 +", "+ p._1._2 +"))").fold("")(_+_) + "]"

        println(batchToPrint +"\n"+ globalToPrint)
      }


    } else if (execType.contains("approx")) {
      var global: List[(String, Int)] = List()
      val w: Int = Math.ceil(Math.exp(1.0)/epsilon).toInt
      val d: Int = Math.ceil(Math.log(1/delta)).toInt
      var cmSketch: Array[Array[Int]] = Array.fill(d){Array.fill(w){0}}
      val r = scala.util.Random
      val seeds: List[Int] = (0 until d).map(_ => r.nextInt()).toList

      words.foreachRDD{rdd =>
        val currentBatch: Array[(String, Int)] = rdd.map{ips => (ips.toString, 1)}.reduceByKey(_+_).collect()

        currentBatch.foreach{ips =>
          for (i <- seeds.indices) {
            val index = Math.abs(stringHash(ips._1, seeds(i)) % w)
            cmSketch(i)(index) += 1
          }
        }

        // retrieve the top K elements and update the global value
        val batchTopK: Array[(String, Int)] = currentBatch.sortWith(_._2 > _._2).take(TOPK)
        global = (global ++ batchTopK.toSeq).groupBy(_._1).mapValues(_.map(_._2).sum).toList.sortWith(_._2 > _._2).take(TOPK)


        val batchToPrint: String = "This batch: ["+ batchTopK.map(p => "( "+ p._2 +","+ p._1 +")").fold("")(_+_) + "]"
        val globalToPrint: String = "Global :["+ global.map(p => "( "+ p._2 + ","+ p._1 +")").fold("")(_+_) + "]"

        println(batchToPrint +"\n"+ globalToPrint)
      }

    }

    // Start the computation
    ssc.start()
  
    // Wait for the computation to terminate
    ssc.awaitTermination()
  }

  def retrieveValueInSketch(seeds: List[Int], cmSketch: Array[Array[Int]], s: String): Int = {
    var min = Int.MaxValue

    for (i <- seeds.indices) {
      val index = Math.abs(stringHash(s, seeds(i)) % cmSketch(0).length)
      val value = cmSketch(i)(index)

      if (value < min)
        min = value
    }

    min
  }
}