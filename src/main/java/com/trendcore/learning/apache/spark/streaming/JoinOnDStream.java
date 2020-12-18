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

import java.util.Arrays;

public class JoinOnDStream {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);


        SparkConf sparkConf = new SparkConf().setAppName("Spark Streaming").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaStreamingContext streamingContext = new JavaStreamingContext(javaSparkContext,
                Durations.seconds(10));


        /*
            When called on two DStreams of (K, V) and (K, W) pairs,
            return a new DStream of (K, (V, W)) pairs
            with all pairs of elements for each key.
         */

        JavaReceiverInputDStream<String> stream1 = streamingContext.socketTextStream("localhost", 9000);

        JavaReceiverInputDStream<String> stream2 = streamingContext.socketTextStream("localhost", 9001);

        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream = stream1
                                    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                                    .mapToPair(s -> new Tuple2<>(s, 1))
                                    ;

        JavaPairDStream<String, String> streamWithHardCodedValues = stream2
                                                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                                                .mapToPair(s -> new Tuple2<>(s, "HardCoded"))
                                                ;

        stringIntegerJavaPairDStream
                .join(streamWithHardCodedValues)
                .print();


        streamingContext.start();
        streamingContext.awaitTermination();

    }

}
