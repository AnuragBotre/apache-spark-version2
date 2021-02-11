package com.trendcore.learning.apache.spark.hive;

import org.apache.spark.sql.SparkSession;

public class HiveWithMetaStoreLocationOnHadoop {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                //.master("local[*]")
                .appName("Java Spark Hive Example")
                //not required
                .config("spark.sql.warehouse.dir", "hdfs://0.0.0.0:9000/user/hive/warehouse/")
                .enableHiveSupport()
                .getOrCreate();


        spark.sql("show tables").show();



    }

}
