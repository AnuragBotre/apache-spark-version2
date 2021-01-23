package com.trendcore.learning.apache.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class ViewDataInEachPartition {

    public static void main(String[] args) {
        SparkConf wordCountSparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Max Element using Reduction");

        JavaSparkContext sparkContext = new JavaSparkContext(wordCountSparkConf);

        JavaRDD<String> stringJavaRDD = sparkContext.textFile("in/word_count.text", 10);


        /*
            View Data on partition.
            Note : map and flatmap operation will not accept partition parameter
            it will take partition parameter from parent RDD.
         */
        stringJavaRDD/*.mapPartitionsWithIndex((v1, v2) -> {
            System.out.println(Thread.currentThread().getName() + " Partition Index :- " + v1);
            v2.forEachRemaining(s -> {
                System.out.println(Thread.currentThread().getName() + " Partition Index :- " + v1 + " " + s);
            });
            return v2;
        }, true)*/
                .flatMap(s -> {
                    return Arrays.stream(s.split(" ")).iterator();
                }).mapPartitionsWithIndex((v1, v2) -> {
            System.out.println(Thread.currentThread().getName() + " Partition Index :- " + v1);
            v2.forEachRemaining(s -> {
                System.out.println(Thread.currentThread().getName() + " Partition Index :- " + v1 + " " + s);
            });
            return v2;
        }, true)
                .foreachPartition(stringIterator -> {

                });


    }

}
