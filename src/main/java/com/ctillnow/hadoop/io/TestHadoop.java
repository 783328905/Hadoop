package com.ctillnow.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;


import java.io.IOException;

/*
*
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/17 19:57
 * 4
*/


public class TestHadoop extends Configured {

    @Test
    public void testHadoop() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.25.164:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("hdfs://192.168.25.164:8020/usr"));
        IOUtils.copyBytes(fsDataInputStream,System.out,new Configuration());
        IOUtils.closeStream(fsDataInputStream);



    }
}
