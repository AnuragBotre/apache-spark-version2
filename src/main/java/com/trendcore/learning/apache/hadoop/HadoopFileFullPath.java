package com.trendcore.learning.apache.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;

public class HadoopFileFullPath {

    public static void main(String[] args) throws IOException {
        Configuration hadoopConf = new Configuration();

        //This value is present in <HADOOP_HOME>/etc/hadoop/core-site.xml
        hadoopConf.set("fs.default.name", "hdfs://0.0.0.0:9000");

        FileSystem hdfs = FileSystem.get(hadoopConf);

        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = hdfs.listFiles(new Path("/"), true);

        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus locatedFileStatus = locatedFileStatusRemoteIterator.next();
            System.out.println(locatedFileStatus.getPath());
        }


    }

}
