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

public class FirstStreamProcessing {


    public static void main(String[] args) throws InterruptedException {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);


        SparkConf sparkConf = new SparkConf().setAppName("Spark Streaming").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaStreamingContext streamingContext = new JavaStreamingContext(javaSparkContext,
                                                        Durations.seconds(10));


        /*
            Use anurag@anurag-pc:~/Desktop$ nc -lk 9999
               command to send text data to this socket.

               Certain methods are available on streamingContext.
               Hence streamingContext object is required.
         */

        JavaReceiverInputDStream<String> dStream = streamingContext.socketTextStream("localhost", 9999);

        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream = dStream
                .flatMap(s -> {
                    String[] s1 = s.split(" ");
                    return Arrays.asList(s1).iterator();
                })
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((v1, v2) -> v1 + v2);


        stringIntegerJavaPairDStream.foreachRDD((rdd, timeUnits) -> {

            System.out.println("timeUnits --->" + timeUnits);

            rdd.foreachPartition(tuple2Iterator -> {
                /*
                    save data to third party system pseudo code
                  Connection connection = Connection to System
                  Dont use for each method as it will create and close connection for each record
                  and is not performance based solution.
                 */

                tuple2Iterator.forEachRemaining(tuple2LongTuple2 -> {
                    System.out.println(tuple2LongTuple2._1 + " " + tuple2LongTuple2._2);
                    /*
                    connection.send(tuple2LongTuple2);
                     */
                });

                /*
                    connection.close
                 */

            });
        });


        /*
            without below 2 lines above program will not run
            Spark streaming will only start computation when it is started.
            Below 2 lines will start computation
         */
        streamingContext.start();              // Start the computation
        streamingContext.awaitTermination();   // Wait for the computation to terminate
    }
}
