package com.trendcore.learning.apache.spark.dataframe;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;
import java.util.Scanner;

public class JdbcDataFrameWithOnePartition {

    public static void main(String[] args) {
        //SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
        SparkSession sparkSession = SparkSession.builder().getOrCreate();

        /*
            When this example is executed on Standalone spark master
            dataframe and RDD is executed on one of the worker node.
            Since partition was only 1
            Spark Worker (Executor process on worker node) will read data
            from mysql.
         */

        /*
            Spark Submit Command :-
            spark-submit --driver-java-options -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
            --master spark://192.168.0.104:7077
            --class com.trendcore.learning.apache.spark.streaming.CountByValueOnDStreams target/apache-spark-version2-1.0-SNAPSHOT.jar
         */

        String jdbcUrl = "jdbc:mysql://localhost:3306/sakila";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "anurag");
        connectionProperties.put("password", "anurag");
        connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver");

        Dataset<Row> actorsDataFrame = sparkSession
                .read()
                .jdbc(jdbcUrl, "actor", connectionProperties)
                .as("actors");


        actorsDataFrame.createOrReplaceTempView("actors");

        Dataset<Row> sql = sparkSession.sql("SELECT * FROM actors ");

        JavaRDD<Row> rowJavaRDD = sql.toJavaRDD();

        rowJavaRDD.mapPartitionsWithIndex((v1, v2) -> {
            System.out.println(Thread.currentThread().getName() + " Partition Index :- " + v1);
            v2.forEachRemaining(s -> {
                System.out.println(Thread.currentThread().getName() + " Partition Index :- " + v1 + " " + s);
            });
            return v2;
        },true).foreachPartition(rowIterator -> {});


        Scanner scanner = new Scanner(System.in);
        scanner.next();
    }

}
