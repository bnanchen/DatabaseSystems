package cube

import org.scalatest._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._

import cubeoperator.{CubeOperator, Dataset}
import org.apache.spark.rdd.RDD

class CubeTest extends FlatSpec {
  val reducers = 10

  val inputFile = "src/test/resources/lineorder_small.tbl"
  val output = "output"

  val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
  val ctx = new SparkContext(sparkConf)
  val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

  val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", "|")
    .load(inputFile)

  val rdd = df.rdd

  val schema = df.schema.toList.map(x => x.name)

  val dataset = new Dataset(rdd, schema)

  test()

  def test() = {
    val cb = new CubeOperator(reducers)

    val groupingList = List("lo_suppkey", "lo_shipmode", "lo_orderdate")
    val res = cb.cube(dataset, groupingList, "lo_supplycost", "SUM")

    //Perform the same query using SparkSQL
    val q1 = df.cube("lo_suppkey","lo_shipmode","lo_orderdate")
      .agg(sum("lo_supplycost") as "sum supplycost")

    println("MOI")
    res.foreach(println(_))

    println("SPARK SQL")
    q1.show(119) // 11976

    // Verify correct number of lines
    assert(q1.rdd.count() == res.count())

    // Verify correct count number
    val q2 = q1.agg(sum("sum supplycost"))
    val value = q2.rdd.map(_.toSeq.toList.map(_.toString())).collect()(0)(0).toDouble
    val valueToVerify = res.map{x => x._2}.fold(0.0)(_+_)
    assert(value == valueToVerify)
  }
}
