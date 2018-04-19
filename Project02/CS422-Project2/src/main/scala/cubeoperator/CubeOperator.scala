package cubeoperator

import org.apache.spark.rdd.RDD

class CubeOperator(reducers: Int) {

  /*
 * This method gets as input one dataset, the grouping attributes of the cube (CUBE BY clause)
 * the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = string, value = double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */
  def cube(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)

    // println("Indices: "+ index) 4,5,16

    val tempRdd: RDD[List[String]] = rdd.map(_.toSeq.toList.map(_.toString()))
    val mappedRdd: RDD[(String, Double)] = map(tempRdd, agg, index, indexAgg)
    val finalRdd: RDD[(String, Double)] = reduce(mappedRdd, agg)

    //TODO Task 1
    finalRdd.foreach(println(_))
    println("AUREVOIR!")
    return finalRdd
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    //TODO naive algorithm for cube computation
    null
  }

  def map(rdd: RDD[List[String]], agg: String, index: List[Int], indexAgg: Int): RDD[(String, Double)] = {
    val rddKeyValue: RDD[(String, List[String])] = rdd.map{x: List[String] => (index.flatMap(x(_)).foldLeft(""){(acc, a) => acc + a}, x)}
    val returnedRDD: RDD[(String, Double)] = {
      agg match {
        case "COUNT" => rddKeyValue.map{kv => (kv._1, 1)}
        case "SUM" | "AVG" => rddKeyValue.map{kv => (kv._1, kv._2(indexAgg).toDouble)} // TODO correct?!?
        case "MIN" | "MAX" => rddKeyValue.map{kv => (kv._1, kv._2(indexAgg).toDouble)}
      }
    }
    returnedRDD.foreach(println(_))
    returnedRDD
  }

  def reduce(rdd: RDD[(String, Double)], agg: String): RDD[(String, Double)] = {
    // TODO can I do that?
    val returnedRDD = rdd.groupByKey().mapValues(values => agg match {
      case "COUNT" | "SUM" => values.sum
      case "AVG" => values.sum / values.size
      case "MIN" => values.min
      case "MAX" => values.max
    })
    returnedRDD
  }

}
