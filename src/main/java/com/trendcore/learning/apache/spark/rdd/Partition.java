package com.trendcore.learning.apache.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Scanner;

public class Partition {

    public static void main(String[] args) {
        SparkConf wordCountSparkConf = new SparkConf()
                .setAppName("WordCount")
                .setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(wordCountSparkConf);

        /*
            Given 100 min partition you will see 100 task in Spark UI.

         */
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("in/word_count.text", 100);

        stringJavaRDD
            .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
            .mapToPair(word -> new Tuple2<>(word,1))
            .groupByKey()
            .foreachPartition(tuple2Iterator -> {
                tuple2Iterator.forEachRemaining(stringIterableTuple2 -> {
                    System.out.println(Thread.currentThread() + " " + stringIterableTuple2);
                });
            })
            ;

        Scanner scanner = new Scanner(System.in);
        scanner.next();

    }


}
