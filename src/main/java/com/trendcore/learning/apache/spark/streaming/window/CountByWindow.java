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

public class CountByWindow {

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

        streamingContext.checkpoint("checkpoint");

        JavaReceiverInputDStream<String> dStream = streamingContext.socketTextStream("localhost", 9999);

        /*
            Will return count over period of window. If window interval is 30 sec
            then it will return count over 30 sec.
            and if slide duration is 10 sec then it will give count on next 10 sec
         */
        dStream

                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(s -> new Tuple2<>(s,1))
                /*
                    window sliding is multiple of 10 (which is parent batch duration)
                 */
                .countByValueAndWindow(Durations.seconds(30),Durations.seconds(20))
                .print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
