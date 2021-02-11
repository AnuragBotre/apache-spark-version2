package com.trendcore.learning.apache.spark.hive;

import com.scala.trendcore.learning.apache.spark.ToRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import scala.reflect.ClassManifestFactory;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class HiveProgram {

    // $example on:spark_hive$
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
    // $example off:spark_hive$

    public static void main(String[] args) {

        /*
            When not configured by the hive-site.xml,
            the context automatically creates metastore_db in the current directory and
            creates a directory configured by spark.sql.warehouse.dir,
            which defaults to the directory spark-warehouse in the current
             directory that the Spark application is started
         */
        /*

                val spark = SparkSession
                                .builder()
                                .appName("ApplicationName")
                                .master("yarn")
                                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                .config("spark.executor.memory", "48120M")
                                .config("hive.metastore.warehouse.dir", "hdfs://ip-10-129-224-21.eu-west-1.compute.internal:8020/user/hive/warehouse")
                                .enableHiveSupport()
                                .getOrCreate();

         */

        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Java Spark Hive Example")
                //not required
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");
        spark.sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src");

        // Queries are expressed in HiveQL
        spark.sql("SELECT * FROM src").show();

        // Aggregation queries are also supported.
        spark.sql("SELECT COUNT(*) FROM src").show();

        // The results of SQL queries are themselves DataFrames and support all normal functions.
        Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");


        Dataset<String> map1 = sqlDF.toDF().map((MapFunction<Row, String>) value -> {
            return "Key: " + value.get(0) + ", Value: " + value.get(1);
        }, Encoders.STRING());

        //return "Key: "+value.get(0)+", Value: "+value.get(1);

        map1.show();

        List<Record> recordList = new ArrayList<>();
        for (int i = 1; i < 100; i++) {
            Record r = new Record();
            r.setKey(i);
            r.setValue("val_"+i);
            recordList.add(r);
        }

        RDD<Record> recordRDD = ToRDD.toRdd(spark.sparkContext(), recordList, ClassManifestFactory.classType(Record.class));

        Dataset<Row> recordDataFrame = spark.createDataFrame(recordRDD, Record.class);
        recordDataFrame.createOrReplaceTempView("records");

        spark.sql("SELECT * FROM records r JOIN src s ON s.key = r.key").show();

    }

}
