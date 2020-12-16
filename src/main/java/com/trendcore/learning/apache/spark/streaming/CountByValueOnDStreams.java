package com.trendcore.learning.apache.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

public class CountByValueOnDStreams {

    public static void main(String[] args) throws InterruptedException {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("Count By Value").setMaster("local[*]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaStreamingContext javaStreamingContext
                = new JavaStreamingContext(javaSparkContext, Durations.seconds(5));

        JavaReceiverInputDStream<String> dStream = javaStreamingContext.socketTextStream("localhost", 9999);


        /*
            Count number of words per string.
            How many times string repeated
            ex:- This is This
            with flatmap line will get transformed to words

         */

        JavaPairDStream<String, Long> stringLongJavaPairDStream
                = dStream
                    .flatMap(v1 -> Arrays.asList(v1.split(" ")).iterator())
                    .countByValue()
                ;

        stringLongJavaPairDStream.foreachRDD((rdd, timeUnits) -> {
            System.out.println("TimeUnits :- " + timeUnits);
            rdd.foreachPartition(tuple2Iterator -> {
                tuple2Iterator.forEachRemaining(stringLongTuple2 -> {
                    System.out.println(stringLongTuple2._1 + " " + stringLongTuple2._2);
                });
            });
        });

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }

}
