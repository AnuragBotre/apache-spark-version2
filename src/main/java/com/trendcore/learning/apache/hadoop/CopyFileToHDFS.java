package com.trendcore.learning.apache.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class CopyFileToHDFS {

    public static void main(String[] args) throws IOException {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.default.name", "hdfs://0.0.0.0:9000");
        FileSystem hdfs = FileSystem.get(hadoopConf);
        Path srcPath = new Path("in/word_count.text");
        Path destPath = new Path("in/word_count.text");
        hdfs.copyFromLocalFile(srcPath, destPath);

    }

}
