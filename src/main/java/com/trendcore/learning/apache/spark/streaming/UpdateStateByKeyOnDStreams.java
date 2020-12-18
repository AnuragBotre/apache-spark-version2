package com.trendcore.learning.apache.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class UpdateStateByKeyOnDStreams {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("Count By Value").setMaster("local[*]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaStreamingContext javaStreamingContext
                = new JavaStreamingContext(javaSparkContext, Durations.seconds(5));

        javaStreamingContext.checkpoint("checkpoint");

        JavaReceiverInputDStream<String> dStream = javaStreamingContext.socketTextStream("localhost", 9999);

        /*
            1. https://techvidvan.com/tutorials/spark-streaming-stateful-transformations/

            2. https://databricks.gitbooks.io/databricks-spark-reference-applications/content/logs_analyzer/chapter1/total.html

            These are operations on DStreams that track data across time.
            It defines, uses some data from the previous batch to generate the results for a new batch.

            Till now we have processed word counts in  particular window
            What was total count of words for last 15 mins.

         */

        dStream
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .countByValue()
                .updateStateByKey((list, oldState) -> {

                    /*
                        Explanation :- In particular batch if we did not receive any elements
                        then send previous state only (which might be 0 in case data is not received from first batch).
                        we calculated all the counts from the list and returned updated state.
                     */
                    Long o = (Long) oldState.orElse(0L);

                    long count = list.stream().reduce(o, (aLong, aLong2) -> aLong + aLong2);

                    return Optional.of( count);

                })
                .print();


        /*stringObjectJavaPairDStream.foreachRDD((rdd, timeUnits) -> {
            System.out.println("Timeunites :- " + timeUnits);
            rdd.foreachPartition(tuple2Iterator -> {
                tuple2Iterator.forEachRemaining(stringObjectTuple2 -> {
                    System.out.println(stringObjectTuple2);
                });
            });
        });*/

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }

}
