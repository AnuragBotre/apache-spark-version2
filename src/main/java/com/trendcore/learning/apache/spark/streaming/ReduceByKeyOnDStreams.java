package com.trendcore.learning.apache.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class ReduceByKeyOnDStreams {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf()
                                .setAppName("ReduceByKey On DStreams")
                                .setMaster("local[*]")
                                ;

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaStreamingContext javaStreamingContext
                = new JavaStreamingContext(javaSparkContext, Durations.seconds(5));

        JavaReceiverInputDStream<String> dStream = javaStreamingContext.socketTextStream("localhost", 9999);

        /*
            reduceByKey provides function to compute values
            as countByKeys will make addition of values with same key.
            In reduce by key we can do multiplication (or any cumulative operation)
         */
        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream = dStream
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((v1, v2) -> {
                    return v1 + v2;
                });

        stringIntegerJavaPairDStream.foreachRDD((rdd, timeUnits) -> {
            System.out.println(Thread.currentThread().getName() + " TimeUnits :- "
                    + new SimpleDateFormat("dd-M-yyyy hh:mm:ss").format(new Date(timeUnits.milliseconds()))
            );

            rdd.foreachPartition(tuple2Iterator -> {
                tuple2Iterator.forEachRemaining(stringIntegerTuple2 -> {
                    System.out.println(stringIntegerTuple2._1 + " " + stringIntegerTuple2._2);
                });
            });
        });

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }

}
