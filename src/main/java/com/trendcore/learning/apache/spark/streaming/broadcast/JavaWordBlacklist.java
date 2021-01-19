package com.trendcore.learning.apache.spark.streaming.broadcast;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

public class JavaWordBlacklist {

    private static volatile Broadcast<List<String>> instance = null;

    public static Broadcast<List<String>> getInstance(JavaSparkContext sparkContext) {
        if (instance == null) {
            synchronized (JavaWordBlacklist.class) {
                if (instance == null) {
                    List<String> broadCastVariable = Arrays.asList("a", "b", "c");
                    instance = sparkContext.broadcast(broadCastVariable);
                }
            }
        }
        return instance;
    }


}
