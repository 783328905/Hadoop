package com.ctillnow.hadoop.io;

import lombok.ToString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;


/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/28 16:06
 * 4
 */
public class IntegrityReaderTest extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        Path rpath = new Path(configuration.get("rpath"));
        Path lpath = new Path(configuration.get("lpath"));

        RawLocalFileSystem rawLocalFileSystem = new RawLocalFileSystem();
        rawLocalFileSystem.initialize(URI.create(configuration.get("rpath")),configuration);
        FSDataInputStream fsDataInputStream = rawLocalFileSystem.open(rpath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
        String rline = bufferedReader.readLine();
        System.out.println(rline);
        bufferedReader.close();

        LocalFileSystem localFileSystem = FileSystem.getLocal(configuration);
        FSDataInputStream fsDataInputStream1 = localFileSystem.open(lpath);
        BufferedReader bufferedReader1 = new BufferedReader(new InputStreamReader(fsDataInputStream1));
        String readLine = bufferedReader1.readLine();
        System.out.println(readLine);
        bufferedReader.close();






        return 0;
    }
    public static void main(String args[]) throws Exception {
        ToolRunner.run(new IntegrityReaderTest(),args);
    }
    /*@Test
    public void testLocalFileSystem() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.25.164:8020");
        Path rpath = new Path("abc.txt");
        Path lpath = new Path("hello.txt");

        RawLocalFileSystem rawLocalFileSystem = new RawLocalFileSystem();
        rawLocalFileSystem.initialize(URI.create("abc.txt"),configuration);
        FSDataInputStream fsDataInputStream = rawLocalFileSystem.open(rpath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
        String rline = bufferedReader.readLine();
        System.out.println(rline);
        bufferedReader.close();

        LocalFileSystem localFileSystem = FileSystem.getLocal(configuration);
        FSDataInputStream fsDataInputStream1 = localFileSystem.open(lpath);
        BufferedReader bufferedReader1 = new BufferedReader(new InputStreamReader(fsDataInputStream1));
        String readLine = bufferedReader1.readLine();
        System.out.println(readLine);
        bufferedReader.close();


    }*/
}
