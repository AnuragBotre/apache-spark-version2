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

public class WindowingOnDStream {

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
                /*
                    windowDuration – width of the window; must be a multiple of this DStream's interval.
                    It will return result word counts over the last 30 seconds of data,
                    series of RDD of last 30 sec will be stored in memory.
                */
            .window(Durations.seconds(30))
            .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
            .mapToPair(s -> new Tuple2<>(s,1))
            .print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }

}
