package com.trendcore.learning.apache.spark.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SortByVsOrderBy {

    public static void main(String[] args) {
        /*
            Sort By - Sorts data within partition
            order by - global ordering
         */

        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();

        Dataset<Row> table = sparkSession.sql("CREATE TABLE person (zip_code INT, name STRING, age INT)");

        table.show();


    }

}
