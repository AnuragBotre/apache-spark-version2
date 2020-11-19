package com.trendcore.learning.apache.spark.rdd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class WordCount {

    public static void main(String[] args) throws IOException {

        /*
        //TODO
        Reading and writing data from/to hdfs
        https://0x0fff.com/spark-hdfs-integration/
         */

        /*This snippet needs to be there in order to copy file local file system to hdfs.*/
        /*Configuration hadoopConf = new Configuration();

        FileSystem hdfs = FileSystem.get(hadoopConf);


        Path srcPath = new Path("in/word_count.text");

        Path destPath = new Path("hdfs://<ip>/in/word_count");


        hdfs.copyFromLocalFile(srcPath, destPath);*/

        /**/

        SparkConf wordCountSparkConf = new SparkConf().setAppName("WordCount");

        JavaSparkContext sparkContext = new JavaSparkContext(wordCountSparkConf);

        JavaRDD<String> stringJavaRDD = sparkContext.textFile("hdfs://0.0.0.0:9000/user/anurag/in/word_count.text");

        List<Tuple2<String, Integer>> collect = stringJavaRDD.flatMap(s ->
                Arrays.asList(s.split(" ")).iterator()
        ).mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((v1, v2) -> v1 + v2).collect();

        collect.forEach(stringIntegerTuple2 ->
                System.out.println(stringIntegerTuple2._1 +" " + stringIntegerTuple2._2));

        Scanner scanner  = new Scanner(System.in);
        scanner.next();
    }
}
