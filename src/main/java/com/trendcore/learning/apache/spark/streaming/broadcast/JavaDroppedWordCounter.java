package com.trendcore.learning.apache.spark.streaming.broadcast;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;


public class JavaDroppedWordCounter {

    private static volatile LongAccumulator longAccumulator;

    public static LongAccumulator getInstance(JavaSparkContext sparkContext) {
        if (longAccumulator == null) {
            synchronized (JavaDroppedWordCounter.class) {
                if(longAccumulator == null) {
                    longAccumulator = sparkContext.sc().longAccumulator("wordsInBlackListCounter");
                }
            }
        }
        return longAccumulator;
    }

}
