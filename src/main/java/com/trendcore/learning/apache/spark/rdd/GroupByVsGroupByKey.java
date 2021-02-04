package com.trendcore.learning.apache.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class GroupByVsGroupByKey {

    public static void main(String[] args) {
        SparkConf wordCountSparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(wordCountSparkConf);

        JavaRDD<String> stringJavaRDD = sparkContext.textFile("in/word_count.text");

        stringJavaRDD.flatMap(s ->
                Arrays.asList(s.split(" ")).iterator()
        ).map(s -> new Tuple2<>(s, 1))
        .groupBy(v1 -> {
            return v1._1;
        })
        .foreachPartition(tuple2Iterator -> {
            tuple2Iterator.forEachRemaining(stringIterableTuple2 -> {
                System.out.println(stringIterableTuple2);
            });
        });



        Scanner scanner  = new Scanner(System.in);
        scanner.next();
    }

}
