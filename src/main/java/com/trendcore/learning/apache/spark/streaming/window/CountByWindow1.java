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

public class CountByWindow1 {

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
                /*
                    windowDuration – width of the window; must be a multiple of this DStream's interval.
                    It will return result word counts over the last 30 seconds of data,
                    series of RDD of last 30 sec will be stored in memory.
                    By Default Slide interval will be batch Duration
                    in this case it will 10 sec.
                */
                .window(Durations.seconds(30),Durations.seconds(20))
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(s -> new Tuple2<>(s,1))
                /*
                    Below line will give exception
                    java.lang.IllegalArgumentException: requirement failed: The window duration of
                    ReducedWindowedDStream (30000 ms) must be multiple
                    of the slide duration of parent DStream (20000 ms)
                 */
                //.countByValueAndWindow(Durations.seconds(30),Durations.seconds(10))
                /*
                    Below line will also give exception
                    java.lang.IllegalArgumentException: requirement failed: The slide duration of
                    ReducedWindowedDStream (10000 ms) must be multiple
                    of the slide duration of parent DStream (20000 ms)
                 */
                //.countByValueAndWindow(Durations.seconds(40),Durations.seconds(10))
                //hence these parameters
                .countByValueAndWindow(Durations.seconds(20),Durations.seconds(20))
                .print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }

}
