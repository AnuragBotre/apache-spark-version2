package com.trendcore.learning.apache.spark.util.streaming;

import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;

public class ForEachPartitionIterator implements VoidFunction<Iterator<Tuple2<String, Long>>> {

    @Override
    public void call(Iterator<Tuple2<String, Long>> tuple2Iterator) throws Exception {
        tuple2Iterator.forEachRemaining(stringLongTuple2 -> {
            System.out.println(stringLongTuple2);
        });
    }
}