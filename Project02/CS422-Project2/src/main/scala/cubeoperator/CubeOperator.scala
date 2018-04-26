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
    val mappedRdd: RDD[(List[String], Double)] = map(tempRdd, agg, index, indexAgg)
    val finalRdd: RDD[(String, Double)] = reduce(mappedRdd, agg)

    //TODO Task 1
    return finalRdd
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    //TODO naive algorithm for cube computation
    null
  }

  def map(rdd: RDD[List[String]], agg: String, index: List[Int], indexAgg: Int): RDD[(List[String], Double)] = {
    //val rddKeyValue: RDD[(String, List[String])] = rdd.map{x: List[String] => (index.flatMap(x(_)).foldLeft(""){(acc, a) => acc + a}, x)} // key is a String
    val rddKeyValue: RDD[(List[String], List[String])] = rdd.map{x: List[String] => (index.map(x(_)).foldLeft(List[String]()){(acc, a) => acc.:+(a)}, x)} // the key now is a List[String]
    val returnedRDD: RDD[(List[String], Double)] = {
      agg match {
        case "COUNT" => rddKeyValue.map{kv => (kv._1, 1)}
        case "SUM" | "AVG" | "MIN" | "MAX" => rddKeyValue.map{kv => (kv._1, kv._2(indexAgg).toDouble)} // TODO correct?!?
        //case "MIN" | "MAX" => rddKeyValue.map{kv => (kv._1, kv._2(indexAgg).toDouble)}
      }
    }
    returnedRDD
  }

  def reduce(rdd: RDD[(List[String], Double)], agg: String): RDD[(String, Double)] = {
    val returnedRDD: RDD[(List[String], Double)] = rdd.groupByKey(reducers).mapValues(values => agg match { // TODO reducers argument useful?!
      case "COUNT" | "SUM" => values.sum
      case "AVG" => values.sum / values.size
      case "MIN" => values.min
      case "MAX" => values.max
    }).cache() // TODO useful?

    val partialKeyRDD = returnedRDD.keys.flatMap(k => k.toSet.subsets.map(_.toList)).sortBy(_.size, ascending=false)
    val mapReturnedRDD = returnedRDD.collectAsMap()
    // TODO make something with with an accumulator

    val finalRDD = returnedRDD.flatMap{x => x._1.toSet.subsets.map { a => (a.toList, x._2) } }.groupByKey().map{kv => (kv._1, kv._2.sum)} // List(1,2) -> List(), List(1), List(2), List(1,2)
    finalRDD.foreach(println(_))

//    val finalRDD = partialKeyRDD.flatMap{l => returnedRDD.filter{el => el._1.containsSlice(l)}.map(_._2)}
//    val l3RDD = partialKeyRDD.filter{f => f.size == 3}.distinct()
//    val l2RDD = partialKeyRDD.filter{f => f.size == 2}.distinct()
//    val l1RDD = partialKeyRDD.filter{f => f.size == 1}.distinct()
//    val l0RDD = partialKeyRDD.filter{f => f.isEmpty}.distinct()
//    // TODO j'ai l'impression d'avoir une bonne idée
//    val temp3RDD: RDD[(List[String], Double)] = l3RDD.map{k => (k, returnedRDD.filter{el => el._1.containsSlice(k)}.map(_._2).fold(0.0)(_+_))}
//    val temp2RDD = temp3RDD ++ l2RDD.map{k => (k, temp3RDD.filter{el => el._1.containsSlice(k)}.map(_._2).fold(0.0)(_+_))}
//    val temp1RDD = temp2RDD ++ l1RDD.map{k => (k, temp2RDD.filter{el => el._1.containsSlice(k)}.map(_._2).fold(0.0)(_+_))}
//    val finalRDD = temp1RDD ++ l0RDD.map{k => (k, temp1RDD.filter{el => el._1.containsSlice(k)}.map(_._2).fold(0.0)(_+_))}

    returnedRDD.map{kv => (kv._1.mkString(""), kv._2)}
  }

}
