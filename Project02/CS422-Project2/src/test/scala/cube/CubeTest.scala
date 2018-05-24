package cube

import org.scalatest._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._

import cubeoperator.{CubeOperator, Dataset}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

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

    println("// ------------ TEST SUM ------------")
    val groupingList = List("lo_suppkey", "lo_shipmode", "lo_orderdate")
    val res = cb.cube(dataset, groupingList, "lo_supplycost", "SUM")

    // using cube naive
    val sumCubeNaive = cb.cube_naive(dataset, groupingList, "lo_supplycost", "SUM")

    //Perform the same query using SparkSQL
    val q1 = df.cube("lo_suppkey","lo_shipmode","lo_orderdate")
      .agg(sum("lo_supplycost") as "sum supplycost")

//    println("MOI")
//    res.foreach(println(_))
//
//    println("SPARK SQL")
//    q1.show(119) // 11976

   //println("COUILLE")
   //val q1Row = q1.rdd.map{x => Row(x)}
   //val resRow = res.map{x => Row(x)}
   //q1Row.subtract(resRow).foreach(println)

    // Verify correct number of lines
    assert(q1.rdd.count() === res.count())
    println("TEST 1 passed.")
    assert(q1.rdd.count() === sumCubeNaive.count())
    println("TEST 2 passed.")
//    println(q1.rdd.count() +" "+ res.count())

    // Verify correct count number
    var q2 = q1.agg(sum("sum supplycost"))
    var value = q2.rdd.map(_.toSeq.toList.map(_.toString())).collect()(0)(0).toDouble
    var valueToVerify = res.map{x => x._2}.fold(0.0)(_+_)
    var valueCubeNaiveToVerify = sumCubeNaive.map{x => x._2}.fold(0.0)(_+_)
    assert(value === valueToVerify)
    println("TEST 3 passed.")
    assert(value === valueCubeNaiveToVerify)
    println("TEST 4 passed.")
    println("To be compared "+ value +" "+ valueToVerify)

    println("// ------------ TEST COUNT ------------")
    val resCount = cb.cube(dataset, groupingList, "lo_supplycost", "COUNT")

    val countCubeNaive = cb.cube_naive(dataset, groupingList, "lo_supplycost", "COUNT")

    //Perform the same query using SparkSQL
    val qCount = df.cube("lo_suppkey","lo_shipmode","lo_orderdate")
      .agg(count("lo_supplycost") as "count supplycost")

    // Verify correct number of lines
    assert(qCount.rdd.count() === resCount.count())
    assert(qCount.rdd.count() === countCubeNaive.count())

    // Verify correct count number
    q2 = qCount.agg(sum("count supplycost"))
    value = q2.rdd.map(_.toSeq.toList.map(_.toString())).collect()(0)(0).toDouble
    valueToVerify = resCount.map{x => x._2}.fold(0.0)(_+_)
    valueCubeNaiveToVerify = countCubeNaive.map{x => x._2}.fold(0.0)(_+_)
    assert(value === valueToVerify)
    assert(value === valueCubeNaiveToVerify)
    println("To be compared "+ value +" "+ valueToVerify)

    println("// ------------ TEST MAX ------------")
    val resMax = cb.cube(dataset, groupingList, "lo_supplycost", "MAX")

    val maxCubeNaive = cb.cube_naive(dataset, groupingList, "lo_supplycost", "MAX")

    //Perform the same query using SparkSQL
    val qMax = df.cube("lo_suppkey","lo_shipmode","lo_orderdate")
      .agg(max("lo_supplycost") as "max supplycost")

    // Verify correct number of lines
    assert(qMax.rdd.count() === res.count())
    assert(qMax.rdd.count() === maxCubeNaive.count())

    // Verify correct count number
    q2 = qMax.agg(sum("max supplycost"))
    value = q2.rdd.map(_.toSeq.toList.map(_.toString())).collect()(0)(0).toDouble
    valueToVerify = resMax.map{x => x._2}.fold(0.0)(_+_)
    valueCubeNaiveToVerify = maxCubeNaive.map{x => x._2}.fold(0.0)(_+_)
    assert(value === valueToVerify)
    assert(value === valueCubeNaiveToVerify)
    println("To be compared "+ value +" "+ valueToVerify)

    println("// ------------ TEST MIN ------------")
    val resMin = cb.cube(dataset, groupingList, "lo_supplycost", "MIN")

    val minCubeNaive = cb.cube_naive(dataset, groupingList, "lo_supplycost", "MIN")

    //Perform the same query using SparkSQL
    val qMin = df.cube("lo_suppkey","lo_shipmode","lo_orderdate")
      .agg(min("lo_supplycost") as "min supplycost")

    // Verify correct number of lines
    assert(qMin.rdd.count() === resMin.count())
    assert(qMin.rdd.count() === minCubeNaive.count())

    // Verify correct count number
    q2 = qMin.agg(sum("min supplycost"))
    value = q2.rdd.map(_.toSeq.toList.map(_.toString())).collect()(0)(0).toDouble
    valueToVerify = resMin.map{x => x._2}.fold(0.0)(_+_)
    valueCubeNaiveToVerify = minCubeNaive.map{x => x._2}.fold(0.0)(_+_)
    assert(value === valueToVerify)
    assert(value === valueCubeNaiveToVerify)
    println("To be compared "+ value +" "+ valueToVerify)

    println("// ------------ TEST AVG ------------")
    val resAvg = cb.cube(dataset, groupingList, "lo_supplycost", "AVG")

    val avgCubeNaive = cb.cube_naive(dataset, groupingList, "lo_supplycost", "AVG")

    //Perform the same query using SparkSQL
    val qAvg = df.cube("lo_suppkey","lo_shipmode","lo_orderdate")
      .agg(avg("lo_supplycost") as "avg supplycost")

//    println("MOI")
//    resAvg.foreach(println(_))
//
//    println("SPARK SQL")
//    qAvg.show(119) // 11976

    // Verify correct number of lines
    assert(qAvg.rdd.count() === resAvg.count())
    assert(qAvg.rdd.count() === avgCubeNaive.count())
    println("Number of lines "+ qAvg.rdd.count() +" "+ resAvg.count())

    // Verify correct count number
    q2 = qAvg.agg(sum("avg supplycost"))
    value = q2.rdd.map(_.toSeq.toList.map(_.toString())).collect()(0)(0).toDouble
    valueToVerify = resAvg.map{x => x._2}.fold(0.0)(_+_)
    valueCubeNaiveToVerify = avgCubeNaive.map{x => x._2}.fold(0.0)(_+_)
    println(valueToVerify +" === "+ valueCubeNaiveToVerify +" === "+ value)
    //assert(valueToVerify === valueCubeNaiveToVerify)
    //println("TEST 1 passed")
    //assert(value === valueCubeNaiveToVerify)
    //println("TEST 2 passed")
    //assert(value === valueToVerify)
    //println("TEST 3 passed")
    //println("To be compared "+ value +" "+ valueToVerify)
  }
}
