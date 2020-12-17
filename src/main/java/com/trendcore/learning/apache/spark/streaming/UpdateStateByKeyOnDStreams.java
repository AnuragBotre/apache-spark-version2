package com.trendcore.learning.apache.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StateSpecImpl;
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
            https://techvidvan.com/tutorials/spark-streaming-stateful-transformations/

            These are operations on DStreams that track data across time.
            It defines, uses some data from the previous batch to generate the results for a new batch.

            Till now we have processed word counts in  particular window
            What was total count of words for last 15 mins.

         */

        JavaPairDStream<String, Object> stringObjectJavaPairDStream = dStream
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))

                .updateStateByKey((list, previousStateValue) -> {
                    System.out.println("Inside previousStateValue :- " + previousStateValue);

                    Optional newStateValue;
                    if (previousStateValue.isPresent()) {
                        Integer o = (Integer) previousStateValue.get();
                        int i = o + 1;
                        newStateValue = Optional.of(i);
                    } else {
                        newStateValue = Optional.of(1);
                    }
                    return newStateValue;
                });


        stringObjectJavaPairDStream.foreachRDD((rdd, timeUnits) -> {
            System.out.println("Timeunites :- " + timeUnits);
            rdd.foreachPartition(tuple2Iterator -> {
                tuple2Iterator.forEachRemaining(stringObjectTuple2 -> {
                    System.out.println(stringObjectTuple2);
                });
            });
        });

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }

}
