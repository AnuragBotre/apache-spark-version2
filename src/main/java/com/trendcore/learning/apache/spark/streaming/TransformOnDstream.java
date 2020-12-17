package com.trendcore.learning.apache.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class TransformOnDstream {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("Count By Value").setMaster("local[*]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaStreamingContext javaStreamingContext
                = new JavaStreamingContext(javaSparkContext, Durations.seconds(5));

        JavaReceiverInputDStream<String> dStream = javaStreamingContext.socketTextStream("localhost", 9999);

        /*
            Applies transformation function on each of the rdd in this DStream.
         */
        JavaDStream<Tuple2<String, Integer>> transformedDStream = dStream.transform((rdd, timeUnits) -> {

            return rdd.flatMap(lines -> {
                return Arrays.asList(lines.split(" ")).iterator();
            }).map(word -> new Tuple2<>(word, 1));
        });


        transformedDStream.foreachRDD((v1, timeUnits) -> {
            System.out.println("Timeunit :- "+timeUnits);
            v1.foreachPartition(tuple2Iterator -> {
                tuple2Iterator.forEachRemaining(s -> System.out.println(s));
            });
        });

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }

}
