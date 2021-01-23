package com.trendcore.learning.apache.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class GroupByImplementationUsingAggregate {

    public static void main(String[] args) {
        SparkConf wordCountSparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Max Element using Reduction");

        JavaSparkContext sparkContext = new JavaSparkContext(wordCountSparkConf);

        JavaRDD<String> stringJavaRDD = sparkContext.textFile("in/word_count.text");

        Map<String, Integer> aggregate = stringJavaRDD
                .flatMap(s ->
                        Arrays.asList(s.split(" ")).iterator()
                )
                .map(s -> new Tuple2<>(s, 1))
                .aggregate(new HashMap<String, Integer>(),
                        (v1, v2) -> {

                            Integer integer = Optional.ofNullable(v1.get(v2._1)).orElse(0);
                            //System.out.println(v2);
                            v1.put(v2._1, integer + v2._2);

                            return v1;
                        }, (v1, v2) -> {

                            Iterator<String> iterator1 = v1.keySet().iterator();

                            Iterator<String> iterator2 = v2.keySet().iterator();

                            //2 for loops
                            //1st will process map1 and update it map2
                            while (iterator1.hasNext()) {
                                String element = iterator1.next();
                                Integer integer1 = v1.get(element);
                                Integer integer2 = Optional.ofNullable(v2.get(element)).orElse(0);
                                v1.put(element, integer1 + integer2);
                            }

                            while (iterator2.hasNext()) {
                                String next = iterator2.next();
                                //not present means not processed
                                if (!v1.containsKey(next)) {
                                    Integer integer = v2.get(next);
                                    v1.put(next, integer);
                                }
                            }

                            return v1;
                        });


        aggregate.forEach((s, integer) -> {
            System.out.println(s + " " + integer);
        });

    }

}
