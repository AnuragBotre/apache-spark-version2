package com.trendcore.learning.apache.spark.rdd;

import com.trendcore.learning.apache.spark.ToRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.reflect.ClassManifestFactory;

import java.util.Arrays;
import java.util.List;

public class MaxUsingReduction {

    public static void main(String[] args) {
        SparkConf wordCountSparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Max Element using Reduction");

        JavaSparkContext sparkContext = new JavaSparkContext(wordCountSparkConf);

        List<Integer> integers = Arrays.asList(1, 2, 3 ,4 ,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20);
        RDD<Integer> trdd = ToRDD.toRdd(sparkContext.sc(), integers, ClassManifestFactory.classType(Integer.class));


        JavaRDD<Integer> integerJavaRDD = JavaRDD.fromRDD(trdd, ClassManifestFactory.classType(Integer.class));

        Integer maxElement = integerJavaRDD.reduce((v1, v2) -> {
            if (v1.compareTo(v2) > 0) {
                return v1;
            } else {
                return v2;
            }
        });

        System.out.println(maxElement);
    }

}
