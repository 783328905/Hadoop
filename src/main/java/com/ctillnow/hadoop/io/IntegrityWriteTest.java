package com.ctillnow.hadoop.io;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.PrintWriter;
import java.net.URI;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/28 14:53
 * 4
 */
public class IntegrityWriteTest extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {

        Configuration configuration = getConf();
        Path rpath = new Path(configuration.get("rpath"));
        Path lpath = new Path(configuration.get("lpath"));

        RawLocalFileSystem rawLocalFileSystem = new RawLocalFileSystem();
        rawLocalFileSystem.initialize(URI.create((configuration.get("rpath"))),configuration);

        FSDataOutputStream outputStream = rawLocalFileSystem.create(rpath);
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("hhhsssss");
        printWriter.flush();
        printWriter.close();

        LocalFileSystem local = FileSystem.getLocal(configuration);
        FSDataOutputStream outputStream1 = local.create(lpath);
        PrintWriter printWriter1 = new PrintWriter(outputStream1);
        printWriter1.print("sfafadf");
        printWriter1.flush();
        printWriter1.close();
        return 0;


    }

   /* @Test
    public void run() throws Exception {

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.25.164:8020");
        Path rpath = new Path("a.txt");
        Path lpath = new Path("b.txt");

        RawLocalFileSystem rawLocalFileSystem = new RawLocalFileSystem();
        rawLocalFileSystem.initialize(URI.create("a.txt"),configuration);

        FSDataOutputStream outputStream = rawLocalFileSystem.create(rpath);
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("hhhsssss");
        printWriter.flush();
        printWriter.close();

        LocalFileSystem local = FileSystem.getLocal(configuration);
        FSDataOutputStream outputStream1 = local.create(lpath);
        PrintWriter printWriter1 = new PrintWriter(outputStream);
        printWriter1.print("sfafadf");
        printWriter1.flush();
        printWriter.close();



    }*/
    public static void main(String args[]) throws Exception {
        ToolRunner.run(new IntegrityWriteTest(),args);
    }
}
