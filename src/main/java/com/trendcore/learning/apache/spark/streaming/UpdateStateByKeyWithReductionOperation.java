package com.trendcore.learning.apache.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class UpdateStateByKeyWithReductionOperation {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("Count By Value").setMaster("local[*]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaStreamingContext javaStreamingContext
                = new JavaStreamingContext(javaSparkContext, Durations.seconds(5));

        javaStreamingContext.checkpoint("checkpoint");

        JavaReceiverInputDStream<String> dStream = javaStreamingContext.socketTextStream("localhost", 9999);

        dStream
                .flatMap(line -> Arrays.stream(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word,1))
                .reduceByKey((v1, v2) -> v1+v2)
                .updateStateByKey((list, oldState) -> {
                    Integer oldValue = (Integer) oldState.orElse(0);
                    Integer reduce = list.stream().reduce(oldValue, (integer, integer2) -> integer + integer2);

                    return Optional.ofNullable(reduce);
                }).print();


        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }

}
