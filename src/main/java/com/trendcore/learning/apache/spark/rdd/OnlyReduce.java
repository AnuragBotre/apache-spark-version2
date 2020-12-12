package com.trendcore.learning.apache.spark.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class OnlyReduce {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf()
                                .setMaster("local[*]")
                                .setAppName("Reduce")
                                ;

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("in/one_word.text");

        /*
            Reduce is only reduce operation ni spark. This is not like Java8 lambda.
            In Java 8 Lambda we can pass in supplier , then accumulator.
            so to reduce by Keys you need pair rdd and can use reduceByKey method.

            reduce method is previous value , current value.

            This method can be used for count operation or simple appending.

            In case of if there is only 1 input then function passed to reduce method will
            not be invoked.

            Simply result from the previous stage (in this map is returned).
         */

        Tuple2 reduce = stringJavaRDD
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .map(v1 -> new Tuple2(v1, 1))
                .reduce((v1, v2) -> {
                    System.out.println(v1._1 + " " + v1._2 + " " + v2._1 + " " + v2._2);
                    return new Tuple2(0, 0);
                })
                ;



        System.out.println(reduce._1 + " " + reduce._2);
    }

}
