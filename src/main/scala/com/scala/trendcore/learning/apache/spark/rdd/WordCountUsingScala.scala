package com.scala.trendcore.learning.apache.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object WordCountUsingScala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Using Scala");
    val sparkContext = new SparkContext(conf);

    /*
      For Extracting variable Ctrl+Alt+v
      Alt+Enter wont work
     */

    val rdd = sparkContext.textFile("in/word_count.text")


    /*
      For Auto completion type Ctrl+Shift+Space for linux  for scala
     */
    rdd.flatMap((str: String) => {
      str.split(" ")
    }).map((str: String) => {
      Tuple2(str, 1)
    }).groupByKey().foreachPartition((tuples: Iterator[(String, Iterable[Int])]) => {
      tuples.foreach((tuple: (String, Iterable[Int])) => {
        println(tuple)
      })
    })


  }


}
