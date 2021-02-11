package com.trendcore.learning.apache;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountCongnologixTest {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("Word count").setMaster("local[*]");

        SparkContext sparkContext = new SparkContext(sparkConf);

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);


        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("in/word_count.text",16);


        Integer reduce = stringJavaRDD
                .flatMap(line -> Arrays.stream(line.split(" ")).iterator())
                .map(word -> 1)
                .reduce((v1, v2) -> v1 + v2);


        System.out.println(reduce);


    }

}
