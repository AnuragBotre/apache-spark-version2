package com.trendcore.learning.apache.spark.hive;

import org.apache.spark.sql.SparkSession;

public class EnableHiveSupport {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .enableHiveSupport()  // <-- enables Hive support
                .getOrCreate();

        /*
            Need below dependency in you pom.xml
            <dependency>
                <groupId>org.apache.spark</groupId>
                <artifactId>spark-hive_2.11</artifactId>
                <version>2.4.7</version>
            </dependency>
         */
        assert(sparkSession.conf().get("spark.sql.catalogImplementation") == "hive");
    }

}
