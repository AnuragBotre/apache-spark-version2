package com.trendcore.learning.apache.spark.rdd;

import com.trendcore.learning.apache.spark.ToRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag;

import java.util.Arrays;
import java.util.List;

public class SparkMasterRDD {

    public static void main(String[] args) {
        SparkConf wordCountSparkConf = new SparkConf()
                .setAppName("WordCount");
        //.setMaster("local[*]");
        //.setMaster("spark://192.168.0.109:7077");
        JavaSparkContext sparkContext = new JavaSparkContext(wordCountSparkConf);

        List<Integer> integers = Arrays.asList(1, 2, 3 ,4 ,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20);
        RDD<Integer> trdd = ToRDD.toRdd(sparkContext.sc(), integers, ClassManifestFactory.<Integer>classType(Integer.class));


        JavaRDD<Integer> integerJavaRDD = JavaRDD.fromRDD(trdd, ClassManifestFactory.<Integer>classType(Integer.class));

        JavaRDD<Integer> repartition = integerJavaRDD.repartition(5);

        repartition.foreach(integer -> {
            System.out.println("Integer " + integer);
        });

    }
}
