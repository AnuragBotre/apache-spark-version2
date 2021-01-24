package com.scala.trendcore.learning.apache.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

object ToRDD {

  def toRdd[T: ClassTag](sparkContext: SparkContext, list: java.util.List[T]): RDD[T] = {
    val seq = list.toSeq
    val rdd = sparkContext.parallelize(seq)
    rdd
  }

}
