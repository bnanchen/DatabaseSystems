package thetajoin

import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import org.apache.spark.sql.Row // TODO on a le droit?


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
    
    // TODO implement the algorithm
    val cs = numS / Math.sqrt(numS * numR / reducers)
    val cr = numR / Math.sqrt(numS * numR / reducers)

    val intAttrRDD1: RDD[Int] = rdd1.map(_(index1).toString.toInt)
    val intAttrRDD2: RDD[Int] = rdd2.map(_(index2).toString.toInt)

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
    
    null
  }  
    
  /*
   * this method takes as input two lists of values that belong to the same partition
   * and performs the theta join on them. Both datasets are lists of tuples (Int, Int)
   * where ._1 is the partition number and ._2 is the value. 
   * Of course you might change this function (both definition and body) if it does not 
   * fit your needs :)
   * */  
  def local_thetajoin(dat1:Iterator[(Int, Int)], dat2:Iterator[(Int, Int)], op:String) : Iterator[(Int, Int)] = {
    var res = List[(Int, Int)]()
    var dat2List = dat2.toList
        
    while(dat1.hasNext) {
      val row1 = dat1.next()      
      for(row2 <- dat2List) {
        if(checkCondition(row1._2, row2._2, op)) {
          res = res :+ (row1._2, row2._2)
        }        
      }      
    }    
    res.iterator
  }  
  
  def checkCondition(value1: Int, value2: Int, op:String): Boolean = {
    op match {
      case "=" => value1 == value2
      case "<" => value1 < value2
      case "<=" => value1 <= value2
      case ">" => value1 > value2
      case ">=" => value1 >= value2
    }
  }
}

