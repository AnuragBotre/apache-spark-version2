package com.scala.trendcore.learning.apache.spark.streams

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

object CountByValueOnDStreams {

  @throws[InterruptedException]
  def main(args: Array[String]) = {
    /*Logger.getLogger("org").setLevel(Level.OFF);
           Logger.getLogger("akka").setLevel(Level.OFF);*/
    //SparkConf sparkConf = new SparkConf().setAppName("Count By Value").setMaster("local[*]");
    val sparkConf = new SparkConf().setAppName("Count By Value").setMaster("spark://0.0.0.0:7077")
    val streamingContext = new StreamingContext(sparkConf, Durations.seconds(5))
    val dStream = streamingContext.socketTextStream("localhost", 9999)

    /*
                Count number of words per string.
                How many times string repeated
                ex:- This is This
                with flatmap line will get transformed to words

             */

    val value = dStream
      .flatMap(_.split(" "))
      .map(new Tuple2[String, Integer](_, 1))
      .countByValue()


    value.foreachRDD(rdd => {
      rdd.foreachPartition(_.foreach(println(_)));
    })


    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
