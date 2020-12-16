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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReduceByKeyOnDStreams2 {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Reduce By Example 2");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaStreamingContext javaStreamingContext
                = new JavaStreamingContext(javaSparkContext, Durations.seconds(5));

        JavaReceiverInputDStream<String> dStream = javaStreamingContext.socketTextStream("localhost", 9999);

        /*
            Receives list from word list
            ex:- This is this
            then using flatmap they are converted words
            For each word ->  Tuple2<>(word,arraylist) is returned
            then it is combined in reduce operation.

            There is no group by operation on  DStream
         */
        JavaPairDStream<String, List> stringListJavaPairDStream = dStream
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> {
                    List list = new ArrayList<>();
                    list.add(1);
                    return new Tuple2<>(word, list);
                })
                .reduceByKey((prevValue, nextValue) -> {
                    prevValue.addAll(nextValue);
                    return prevValue;
                });

        stringListJavaPairDStream.foreachRDD((rdd, timeUnit) -> {
            System.out.println("TimeUnit :- " + timeUnit);
            rdd.foreachPartition(tuple2Iterator -> {
                tuple2Iterator.forEachRemaining(stringListTuple2 -> {
                    System.out.println(stringListTuple2);
                });
            });
        });

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }

}
