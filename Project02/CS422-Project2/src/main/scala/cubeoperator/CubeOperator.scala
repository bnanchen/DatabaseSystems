package cubeoperator

import org.apache.spark.HashPartitioner
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

    val tempRDD: RDD[List[String]] = rdd.map(_.toSeq.toList.map(_.toString()))
    val mappedRDD: RDD[(List[String], (Double, Option[Double]))] = map(tempRDD, agg, index, indexAgg)
    val finalRDD: RDD[(String, Double)] = reduce(mappedRDD, agg)

    finalRDD
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {
    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)

    val tempRDD: RDD[List[String]] = rdd.map(_.toSeq.toList.map(_.toString()))
    val aggValuesRDD: RDD[(List[String], List[String])] = tempRDD.map{x => (index.map(x(_)).foldLeft(List[String]()){(acc, a) => acc.:+(a)}, x)}
    val combinationRDD: RDD[(List[String], List[String])] = aggValuesRDD.flatMap{x => combinator.combinations(x._1, x._1.size).map{a => (a, x._2)}}

    // MAP
    val mappedRDD: RDD[(List[String], (Double, Option[Double]))] = {
      agg match {
        case "COUNT" => combinationRDD.map{kv => (kv._1, (1, None))}
        case "AVG" => combinationRDD.map{kv => (kv._1, (kv._2(indexAgg).toDouble, Some(1.0)))}
        case "SUM" | "MIN" | "MAX" => combinationRDD.map{kv => (kv._1, (kv._2(indexAgg).toDouble, None))}
      }
    }

    // REDUCE
    val reducedRDD: RDD[(List[String], Double)] = {
      agg match {
        case "COUNT" | "SUM" =>
          mappedRDD.map{kv => (kv._1, kv._2._1)}.groupByKey(reducers).map{kv => (kv._1, kv._2.sum)}
        case "AVG" =>
          mappedRDD.map{kv => (kv._1, (kv._2._1, kv._2._2.get))}.groupByKey(reducers).map{kv => (kv._1, kv._2.map(_._1).sum / kv._2.map(_._2).sum)}
        case "MAX" =>
          mappedRDD.map{kv => (kv._1, kv._2._1)}.groupByKey(reducers).map{kv => (kv._1, kv._2.max)}
        case "MIN" =>
          mappedRDD.map{kv => (kv._1, kv._2._1)}.groupByKey(reducers).map{kv => (kv._1, kv._2.min)}
      }
    }

    reducedRDD.map{kv => (kv._1.mkString(","), kv._2)}
  }

  def map(rdd: RDD[List[String]], agg: String, index: List[Int], indexAgg: Int): RDD[(List[String], (Double, Option[Double]))] = {
    val rddKeyValue: RDD[(List[String], List[String])] = rdd.map{x: List[String] => (index.map(x(_)).foldLeft(List[String]()){(acc, a) => acc.:+(a)}, x)} // the key now is a List[String]
    val returnedRDD: RDD[(List[String], (Double, Option[Double]))] = {
      agg match {
        case "COUNT" => rddKeyValue.map{kv => (kv._1, (1, None))}
        case "AVG" => rddKeyValue.map{kv => (kv._1, (kv._2(indexAgg).toDouble, Some(1.0)))}
        case "SUM" | "MIN" | "MAX" => rddKeyValue.map{kv => (kv._1, (kv._2(indexAgg).toDouble, None))}
      }
    }
    returnedRDD.partitionBy(new HashPartitioner(reducers))
  }

  def reduce(rdd: RDD[(List[String], (Double, Option[Double]))], agg: String): RDD[(String, Double)] = {
    val returnedRDD: RDD[(List[String], Double)] = agg match {
      case "SUM" | "COUNT" =>
        rdd.mapPartitions(p => p.flatMap{x => combinator.combinations(x._1, x._1.size).map { a => (a, x._2._1) }}).groupByKey(reducers).map{kv => (kv._1, kv._2.sum)}
      case "MIN" =>
        rdd.mapPartitions(p => p.flatMap{x => combinator.combinations(x._1, x._1.size).map { a => (a, x._2._1) }}).groupByKey(reducers).map{kv => (kv._1, kv._2.min)}
      case "MAX" =>
        rdd.mapPartitions(p => p.flatMap{x => combinator.combinations(x._1, x._1.size).map { a => (a, x._2._1) }}).groupByKey(reducers).map{kv => (kv._1, kv._2.max)}
      case "AVG" =>
        rdd.mapPartitions(p => p.flatMap{x => combinator.combinations(x._1, x._1.size).map { a => (a, (x._2._1, x._2._2.get))}}).groupByKey(reducers).map{kv => (kv._1, kv._2.map(_._1).sum / kv._2.map(_._2).sum)}
    }

    returnedRDD.map{kv => (kv._1.mkString(","), kv._2)}
  }
}

object combinator {
  def combinations(l: List[String], n: Int): List[List[String]] = l match {
    case Nil => List(Nil)
    case x if n == 0 =>
      List(x)
    case x :: xs =>
      combinations(xs, n - 1).map(x :: _) ::: combinations(xs, n).map(null :: _)
    case _ => Nil
  }
}