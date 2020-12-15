package com.trendcore.learning.apache.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class UnionOnDStreams {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Union on Dstreams");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext,Durations.seconds(5));

        JavaReceiverInputDStream<String> firstStream = javaStreamingContext.socketTextStream("localhost", 9000);

        JavaReceiverInputDStream<String> secondStream = javaStreamingContext.socketTextStream("localhost",9100);

        /*
            This program will not run on 2 core machine. As the both the core will be used for listening
         */
        firstStream
        .union(secondStream)
        .foreachRDD((stringJavaRDD, time) -> {
            System.out.println("Printing results :- " + time);
            stringJavaRDD.foreachPartition(iterator -> {
                iterator.forEachRemaining(s -> {
                    System.out.println(s);
                });
            });
        })
        ;

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }

}
