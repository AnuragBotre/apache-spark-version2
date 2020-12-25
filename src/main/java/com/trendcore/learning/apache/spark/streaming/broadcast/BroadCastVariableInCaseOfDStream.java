package com.trendcore.learning.apache.spark.streaming.broadcast;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class BroadCastVariableInCaseOfDStream {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("Count By Value").setMaster("local[*]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaStreamingContext javaStreamingContext
                = new JavaStreamingContext(javaSparkContext, Durations.seconds(10));

        javaStreamingContext.checkpoint("checkpoint");

        JavaReceiverInputDStream<String> dStream = javaStreamingContext.socketTextStream("localhost", 9999);


        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream = dStream
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1));

        stringIntegerJavaPairDStream.foreachRDD((rdds, timeUnit) -> {

            /*
                Filter Some words which are present in broadcast variable
                also count number of words which are skipped using accumulators

                Note :- Accumulators and Broadcast variables cannot be recovered from checkpoint in Spark Streaming.
                If you enable checkpointing and use Accumulators or Broadcast variables as well, we’ll have
                to create lazily instantiated singleton instances
                for Accumulators and Broadcast variables
                so that they can be re-instantiated after the driver restarts on failure.

                This is shown in this example.
             */

            Broadcast<List<String>> tobFilterList = JavaWordBlacklist.getInstance(new JavaSparkContext(rdds.context()));
            LongAccumulator counter = JavaDroppedWordCounter.getInstance(new JavaSparkContext(rdds.context()));

            rdds
                .filter(v1 -> {
                    boolean contains = tobFilterList.getValue().contains(v1._1.trim());
                    if (contains) {
                        counter.add(1);
                        return false;
                    } else {
                        return true;
                    }

                }).foreachPartition(tuple2Iterator -> {
                    tuple2Iterator.forEachRemaining(stringIntegerTuple2 -> System.out.println(stringIntegerTuple2));
                });
            System.out.println("Counter Filtered count :- " + counter);
        });


        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }

}
