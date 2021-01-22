package com.trendcore.learning.apache.spark.rdd;

import com.trendcore.learning.apache.spark.ToRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.reflect.ClassManifestFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class FindMax {

    public static void main(String[] args) {
        SparkConf wordCountSparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Max Element");

        JavaSparkContext sparkContext = new JavaSparkContext(wordCountSparkConf);

        List<Integer> integers = Arrays.asList(1, 2, 3 ,4 ,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20);
        RDD<Integer> trdd = ToRDD.toRdd(sparkContext.sc(), integers, ClassManifestFactory.classType(Integer.class));


        JavaRDD<Integer> integerJavaRDD = JavaRDD.fromRDD(trdd, ClassManifestFactory.classType(Integer.class));

        Integer max = integerJavaRDD.max(new SerializableComparator());

        System.out.println(max);

        int length = trdd.getPartitions().length;

        List<Integer> take = integerJavaRDD.sortBy(v1 -> v1, false, length).take(1);

        take.forEach(integer -> System.out.println(integer));

    }

    private static class SerializableComparator implements java.util.Comparator<Integer> , Serializable {

        @Override
        public int compare(Integer o1, Integer o2) {
            return o1.compareTo(o2);
        }
    }
}
