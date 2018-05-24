package thetajoin

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer // TODO on a le droit?

// class representing a Bucket (or a Region)
/**
  *
  * @param horizontalStart inclusive
  * @param horizontalEnd exclusive
  * @param verticalStart inclusive
  * @param verticalEnd exclusive
  */
case class Bucket(horizontalStart: Int, horizontalEnd: Int, verticalStart: Int, verticalEnd: Int) {
  def contains(index: Long, horizontal: Boolean): Boolean = {
    if (horizontal)
      if (index == 0)
        horizontalStart <= index && index <= horizontalEnd
      else
        horizontalStart < index && index <= horizontalEnd
    else
      if (index == 0)
        verticalStart <= index && index <= verticalEnd
      else
        verticalStart < index && index <= verticalEnd
  }
}

class ThetaJoin(numR: Long, numS: Long, reducers: Int, bucketsize: Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("ThetaJoin")    
  
  // random samples for each relation
  // helper structures, you are allowed
  // not to use them
  var horizontalBoundaries = Array[Int]()
  var verticalBoundaries = Array[Int]()
  
  // number of values that fall in each partition
  // helper structures, you are allowed
  // not to use them
  var horizontalCounts = Array[Int]()
  var verticalCounts = Array[Int]()
  
  /*
   * this method gets as input two datasets and the condition
   * and returns an RDD with the result by projecting only 
   * attr1 and attr2
   * You are not allowed to change the definition of this function.
   * */  
  def theta_join(dataset1: Dataset, dataset2: Dataset, attr1:String, attr2:String, op:String): RDD[(Int, Int)] = {
    val schema1 = dataset1.getSchema()
    val schema2 = dataset2.getSchema()

    val rdd1 = dataset1.getRDD()
    val rdd2 = dataset2.getRDD()

    val index1 = schema1.indexOf(attr1)
    val index2 = schema2.indexOf(attr2)

    // Equi-Depth Histograms
    val cs = numS / Math.sqrt(numS * numR / reducers)
    val cr = numR / Math.sqrt(numS * numR / reducers)

    val intRDD1: RDD[List[Int]] = rdd1.map(_.toSeq.toList.map(_.toString.toInt)).sortBy(_(index1))
    val intRDD2: RDD[List[Int]] = rdd2.map(_.toSeq.toList.map(_.toString.toInt)).sortBy(_(index2))

    val intAttrRDD1: RDD[Int] = intRDD1.map(_(index1))
    val intAttrRDD2: RDD[Int] = intRDD2.map(_(index2))

    horizontalBoundaries = intAttrRDD1.sample(withReplacement = false, cs/numS).collect().sortWith{_<=_}
    verticalBoundaries = intAttrRDD2.sample(withReplacement = false, cr/numR).collect().sortWith{_<=_}

    horizontalCounts = Array.fill(horizontalBoundaries.length+1){0}
    verticalCounts = Array.fill(verticalBoundaries.length+1){0}

    val horizontalBuckets = (Int.MinValue +: horizontalBoundaries).zip(horizontalBoundaries :+ Int.MaxValue)
    val verticalBuckets = (Int.MinValue +: verticalBoundaries).zip(verticalBoundaries :+ Int.MaxValue)

    for {
      i <- horizontalCounts.indices
    } {
      horizontalCounts(i) = intAttrRDD1.filter{el => horizontalBuckets(i)._1 < el && horizontalBuckets(i)._2 >= el}.count().toInt
    }

    for {
      i <- verticalCounts.indices
    } {
      verticalCounts(i) = intAttrRDD2.filter{el => verticalBuckets(i)._1 < el && verticalBuckets(i)._2 >= el}.count().toInt
    }

    // M-Bucket-I
    val intAttrRDD2Size = intAttrRDD2.count()

    val buckets: List[Bucket] = MBucketI(bucketsize, intAttrRDD2Size.toInt, horizontalCounts.toList, verticalCounts.toList, horizontalBuckets, verticalBuckets, op)

    // return a RDD of tuples with the bucket number and the attribute
    val bucketRDD1: RDD[(Int, Int)] = intRDD1.zipWithIndex().map{row => (whichBucket(row._2, buckets, horizontal = true), row._1)}.filter{el => el._1.nonEmpty}.flatMap{x => x._1.map{b => (b, x._2(index1))}}
    val bucketRDD2: RDD[(Int, Int)] = intRDD2.zipWithIndex().map{row => (whichBucket(row._2, buckets, horizontal = false), row._1)}.filter{el => el._1.nonEmpty}.flatMap{x => x._1.map{b => (b, x._2(index2))}}

    val partitionnedRDD1: RDD[(Int, Int)] = bucketRDD1.partitionBy(new HashPartitioner(reducers))
    val partitionnedRDD2: RDD[(Int, Int)] = bucketRDD2.partitionBy(new HashPartitioner(reducers))

    partitionnedRDD1.zipPartitions(partitionnedRDD2){case (x,y) => local_thetajoin(x, y, op)}
  }

  def MBucketI(maxInput: Int, numbRows: Int, horizontalCounts: List[Int], verticalCounts: List[Int], horizontalBuckets: Array[(Int, Int)], verticalBuckets: Array[(Int, Int)], op: String): List[Bucket] = {
    var row = 0
    var buckets: List[Bucket] = List()

    val horizontalIndices: List[(Int, Int)] = indicesM(horizontalCounts, 0)
    val verticalIndices: List[(Int, Int)] = indicesM(verticalCounts, 0)

    while (row < numbRows) {
      val result: (Int, List[Bucket]) = coverSubMatrix(row, bucketsize, horizontalIndices, verticalIndices, horizontalBuckets, verticalBuckets, op)
      row = result._1
      buckets = buckets ::: result._2
    }

    buckets
  }

  def coverSubMatrix(row: Int, maxInput: Int, horizontalIndices: List[(Int, Int)], verticalIndices: List[(Int, Int)], horizontalBuckets: Array[(Int, Int)], verticalBuckets: Array[(Int, Int)], op: String): (Int, List[Bucket]) = {
    var maxScore: Double = -1.0
    var bestBuckets: List[Bucket] = List()
    var bestRow: Int = -1
    var startHorizontal = Int.MaxValue
    var endHorizontal = 0
    var widthCandidateCells = 0
    var candidateCellsCovered: Int = 0

    for (i <- 1 until maxInput
         if row+i <= verticalIndices.last._2) {
      val result = coverRows(row, row+i, maxInput, horizontalIndices, verticalIndices, horizontalBuckets, verticalBuckets, startHorizontal, endHorizontal, widthCandidateCells, candidateCellsCovered, op)
      val buckets: List[Bucket] = result._1
      candidateCellsCovered =  result._2
      startHorizontal = result._3
      endHorizontal = result._4
      widthCandidateCells = result._5

      val score: Double = if (buckets.isEmpty) -1.0 else candidateCellsCovered.toDouble/buckets.size.toDouble

      if (score >= maxScore) {
        bestRow = row + i
        bestBuckets = buckets
        maxScore = score
      }
    }

    (bestRow, bestBuckets)
  }

  def coverRows(rowf: Int, rowl: Int, maxInput: Int, horizontalIndices: List[(Int, Int)], verticalIndices: List[(Int, Int)], horizontalBuckets: Array[(Int, Int)], verticalBuckets: Array[(Int, Int)], oldStartHorizontal: Int, oldEndHorizontal: Int, oldWidth: Int, oldCovered: Int, op: String): (List[Bucket], Int, Int, Int, Int) = {
    var buckets: List[Bucket] = List()
    var notDefined: Boolean = true

    val (widthCandidateCells: Int, candidateCellsCovered: Int, startHorizontal: Int, endHorizontal: Int) = candidateCells(Bucket(0, 0, rowf, rowl), horizontalIndices, verticalIndices, horizontalBuckets, verticalBuckets, oldStartHorizontal, oldEndHorizontal, oldWidth, oldCovered, op)

    var dimension = maxInput

    while (notDefined) {
      if (widthCandidateCells % dimension == 0) {
        notDefined = false
      } else {
        dimension -= 1
      }
    }

    for (column <- startHorizontal until (startHorizontal + widthCandidateCells) by dimension) {
      val bucket = Bucket(column, column + dimension, rowf, rowl)
      buckets = buckets :+ bucket
    }

    (buckets, candidateCellsCovered, startHorizontal, endHorizontal, widthCandidateCells)
  }

  // Compute the number of candidate cells in the area
  def candidateCells(bucket: Bucket, horizontalIndices: List[(Int, Int)], verticalIndices: List[(Int, Int)], horizontalBuckets: Array[(Int, Int)], verticalBuckets: Array[(Int, Int)], accStartHorizontal: Int, accEndHorizontal: Int, accWidth: Int, accCovered: Int, op: String): (Int, Int, Int, Int) = {
    var maxWidth: Int = accWidth
    var candidateCellsCovered: Int = accCovered
    var startHorizontal: Int = accStartHorizontal
    var endHorizontal: Int = accEndHorizontal

    val verticalIndicesToInfinite = verticalIndices.init :+ (verticalIndices.last._1, Int.MaxValue)
    val horizontalIndicesToInfinite = horizontalIndices.init :+ (horizontalIndices.last._1, Int.MaxValue)

    for (i <- bucket.verticalStart until bucket.verticalEnd) { // exclusive
      var width = 0
      var tempStartHorizontal = Int.MaxValue
      var tempEndHorizontal = 0
      // calculate the interval of the vertical bucket
      val verticalInterval: (Int, Int) = verticalBuckets(verticalIndicesToInfinite.indexWhere(el => i >= el._1 && i < el._2))
      var j = 0

      while(j < horizontalIndices.last._2) {
        val bucketIndex: Int = horizontalIndicesToInfinite.indexWhere(el => j >= el._1 && j < el._2)
        // calculate the interval of the horizontal bucket
        val horizontalInterval: (Int, Int) = horizontalBuckets(bucketIndex)

        val numbCells = horizontalIndices(bucketIndex)._2 - horizontalIndices(bucketIndex)._1

        if (isCandidateCell(horizontalInterval, verticalInterval, op)) {
          if (width == 0 && j < startHorizontal) {
            tempStartHorizontal = j
            startHorizontal = j
          } else if (width == 0) {
            tempStartHorizontal = j
          }
          width = width + numbCells
          candidateCellsCovered = candidateCellsCovered + numbCells
        }
        j = j + numbCells
      }

      tempEndHorizontal = tempStartHorizontal + width - 1

      if (width > maxWidth) {
        maxWidth = width
        endHorizontal = startHorizontal + width - 1
      } else if (tempStartHorizontal < startHorizontal) {
        maxWidth = maxWidth + (startHorizontal - tempStartHorizontal)
      } else if (tempEndHorizontal > endHorizontal) {
        maxWidth = maxWidth + (tempEndHorizontal - endHorizontal)
      }
    }

    (maxWidth, candidateCellsCovered, startHorizontal, endHorizontal) // endHorizontal is inclusive
  }

  def isCandidateCell(horizontalInterval: (Int, Int), verticalInterval: (Int, Int), op: String): Boolean = op match {
    case "=" => verticalInterval._1 <= horizontalInterval._2 && horizontalInterval._1 <= verticalInterval._2
    case "!=" => true
    case "<" => horizontalInterval._1 < verticalInterval._2
    case "<=" => horizontalInterval._1 <= verticalInterval._2
    case ">" => horizontalInterval._2 > verticalInterval._1
    case ">=" => horizontalInterval._2 >= verticalInterval._1
  }
    
  /*
   * this method takes as input two lists of values that belong to the same partition
   * and performs the theta join on them. Both datasets are lists of tuples (Int, Int)
   * where ._1 is the partition number and ._2 is the value. 
   * Of course you might change this function (both definition and body) if it does not 
   * fit your needs :)
   * */  
  def local_thetajoin(dat1:Iterator[(Int, Int)], dat2:Iterator[(Int, Int)], op:String) : Iterator[(Int, Int)] = {
    var res = new ListBuffer[(Int, Int)]()
    var dat2List = dat2.toList
        
    while(dat1.hasNext) {
      val row1 = dat1.next()      
      for(row2 <- dat2List) {
        if(row1._1 == row2._1 && checkCondition(row1._2, row2._2, op)) {
          res += ((row1._2, row2._2))
        }        
      }      
    }    
    res.toList.iterator
  }  
  
  def checkCondition(value1: Int, value2: Int, op:String): Boolean = {
    op match {
      case "=" => value1 == value2
      case "!=" => value1 != value2
      case "<" => value1 < value2
      case "<=" => value1 <= value2
      case ">" => value1 > value2
      case ">=" => value1 >= value2
    }
  }

  def indicesM(count: List[Int], acc: Int): List[(Int, Int)] = count match {
    case Nil => Nil
    case x :: xs => (acc, acc+x) :: indicesM(xs, acc+x)
  }

  // return in which bucket(s) the row lies
  def whichBucket(index: Long, buckets: List[Bucket], horizontal: Boolean): List[Int] = {
    var numBuckets: List[Int] = List()
    for (bucket <- buckets) {
      if (bucket.contains(index, horizontal)) {
        numBuckets = numBuckets :+ buckets.indexOf(bucket)
      }
    }
    numBuckets
  }
}

