package com.trendcore.learning.apache.spark.rdd;

import com.scala.trendcore.learning.apache.spark.ToRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassManifestFactory;

import java.util.Arrays;
import java.util.List;

public class AverageUsingReduction {

    public static void main(String[] args) {
        SparkConf wordCountSparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Max Element using Reduction");

        JavaSparkContext sparkContext = new JavaSparkContext(wordCountSparkConf);

        List<Integer> integers = Arrays.asList(1, 2, 3 ,4 ,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20);
        RDD<Integer> trdd = ToRDD.toRdd(sparkContext.sc(), integers, ClassManifestFactory.classType(Integer.class));



        JavaRDD<Integer> integerJavaRDD = JavaRDD.fromRDD(trdd, ClassManifestFactory.classType(Integer.class));

        Tuple2<Integer, Integer> avg = integerJavaRDD
                .map(v1 -> new Tuple2<Integer, Integer>(v1, 1))
                .reduce((v1, v2) -> {
                    return new Tuple2(v1._1 + v2._1, v1._2 + v2._2);
                });

        System.out.println(avg._1 + " " + avg._2);
        System.out.println(avg._1 / avg._2);
    }

}
