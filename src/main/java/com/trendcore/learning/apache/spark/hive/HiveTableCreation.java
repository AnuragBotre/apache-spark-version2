package com.trendcore.learning.apache.spark.hive;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HiveTableCreation {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .enableHiveSupport()  // <-- enables Hive support
                .getOrCreate();
        /*
            https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/sql/hive/SparkHiveExample.scala
         */

        sparkSession.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");

        // You can also use DataFrames to create temporary views within a SparkSession.
        List<Record> records = new ArrayList<>();
        for (int key = 1; key < 100; key++) {
            Record record = new Record();
            record.setKey(key);
            record.setValue("val_" + key);
            records.add(record);
        }
        Dataset<Row> recordsDF = sparkSession.createDataFrame(records, Record.class);
        recordsDF.createOrReplaceTempView("records");

        // Queries can then join DataFrames data with data stored in Hive.
        sparkSession.sql("SELECT * FROM records r LEFT JOIN src s ON r.key = s.key").show();

    }

    public static class Record implements Serializable {
        private int key;
        private String value;

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }


}
