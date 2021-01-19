package com.trendcore.learning.apache.spark.streaming.dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

public class DataFrameOnDStream {

    public static class Word{
        private String word;

        private long count;

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);


        SparkConf sparkConf = new SparkConf().setAppName("Spark Streaming").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaStreamingContext streamingContext = new JavaStreamingContext(javaSparkContext,
                Durations.seconds(10));

        SparkSession sparkSession = SparkSession.builder().getOrCreate();

        /*
               Certain methods are available on streamingContext.
               Hence streamingContext object is required.
         */

        JavaReceiverInputDStream<String> dStream = streamingContext.socketTextStream("localhost", 9999);

        JavaDStream<String> stringJavaDStream = dStream
                .flatMap(s -> {
                    String[] s1 = s.split(" ");
                    return Arrays.asList(s1).iterator();
                });


        stringJavaDStream.foreachRDD((stringIntegerJavaPairRDD, timeUnit) -> {

            JavaRDD<Word> wordRdd = stringIntegerJavaPairRDD.map(word -> {
                Word record = new Word();
                record.setWord(word);
                record.setCount(1);
                return record;
            });

            Dataset<Row> dataFrame = sparkSession.createDataFrame(wordRdd.rdd(), Word.class);

            dataFrame.createOrReplaceTempView("words");

            Dataset<Row> sql = sparkSession.sql("select word,count(*) from words w group by w.word");

            sql.foreach(row -> {
                System.out.println(row.get(0) + " " + row.get(1));
            });

        });

        streamingContext.start();              // Start the computation
        streamingContext.awaitTermination();   // Wait for the computation to terminate
    }
}
