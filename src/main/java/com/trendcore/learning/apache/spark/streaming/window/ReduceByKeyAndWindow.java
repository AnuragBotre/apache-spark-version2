package com.trendcore.learning.apache.spark.streaming.window;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class ReduceByKeyAndWindow {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);


        SparkConf sparkConf = new SparkConf().setAppName("Spark Streaming").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaStreamingContext streamingContext = new JavaStreamingContext(javaSparkContext,
                Durations.seconds(10));


        /*
             Return a new DStream which is computed based on windowed batches of the source DStream.
         */

        JavaReceiverInputDStream<String> dStream = streamingContext.socketTextStream("localhost", 9999);

        dStream
            .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
            .mapToPair(word -> new Tuple2<>(word,1))
            .reduceByKeyAndWindow((v1, v2) -> {
                    return v1 + v2;
                },Durations.seconds(20)
            )
            .print();

        streamingContext.start();              // Start the computation
        streamingContext.awaitTermination();   // Wait for the computation to terminate
    }

}
