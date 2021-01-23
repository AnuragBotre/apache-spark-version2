package com.trendcore.learning.apache.spark.dataframe;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class JdbcDataFrameWithOnePartition {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();

        String jdbcUrl = "jdbc:mysql://localhost:3306/sakila";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "anurag");
        connectionProperties.put("password", "anurag");

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



    }

}
