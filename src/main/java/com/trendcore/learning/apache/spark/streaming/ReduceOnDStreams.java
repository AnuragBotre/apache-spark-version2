package com.trendcore.learning.apache.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class ReduceOnDStreams {

    public static void main(String[] args) throws InterruptedException {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("Reduce on DStream").setMaster("local[*]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext,
                Durations.seconds(10));

        JavaReceiverInputDStream<String> dStream = javaStreamingContext.socketTextStream("localhost", 9999);

        /*
            Count the Number of words in stream for each window.
         */

        JavaDStream<Integer> reduceDStream = dStream
                .map(str -> str.split(" " ).length)
                .reduce((previousObject, currentObject) -> {
                            return previousObject + currentObject;
                        }
                )
                ;

        /*
            Print word count in each window for processed RDD
         */
        reduceDStream.foreachRDD((rdd, timeUnit) -> {

            if(rdd.isEmpty()) {
                /*
                    Calling reduce method on empty RDD results in below exception.
                    java.lang.UnsupportedOperationException: empty collection
                 */
                System.out.println("RDD is empty");
            } else {
                Integer result = rdd.reduce((previousObject, currentObject) -> {
                    return previousObject + currentObject;
                });
                System.out.println("TimeUnit :- " + timeUnit + " No of words in RDD for given window :- " + result);
            }




        });

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }

}
