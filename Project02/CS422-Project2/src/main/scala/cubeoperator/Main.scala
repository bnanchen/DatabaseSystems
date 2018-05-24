package cubeoperator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._

object Main {
  def main(args: Array[String]) {

    val reducers = 100

    val inputFile = "src/test/resources/lineorder_medium.tbl"
    val output = "output"

    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]") // TODO .setMaster() was commented
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

    val cb = new CubeOperator(reducers)

    val groupingList = List("lo_suppkey","lo_shipmode","lo_orderdate")

    val t1 = System.nanoTime()

    val res = cb.cube(dataset, groupingList, "lo_supplycost", "AVG")

    //val cubeNaive = cb.cube_naive(dataset, groupingList, "lo_supplycost", "AVG")

    println("TIME: "+ (System.nanoTime()-t1)/(Math.pow(10,9)))

    /*
       The above call corresponds to the query:
       SELECT lo_suppkey, lo_shipmode, lo_orderdate, SUM (lo_supplycost)
       FROM LINEORDER
       CUBE BY lo_suppkey, lo_shipmode, lo_orderdate
     */

    //res.saveAsTextFile(output)

    //Perform the same query using SparkSQL
    //    val q1 = df.cube("lo_suppkey","lo_shipmode","lo_orderdate")
    //      .agg(sum("lo_supplycost") as "sum supplycost")
    //    q1.show


  }
}