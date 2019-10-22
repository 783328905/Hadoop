package com.ctillnow.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;


/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/30 14:59
 * 4 生成序列文件
 */
public class GenerateSeqFile {
    @Test
    public void save() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.25.164:8020");
        FileSystem fileSystem = FileSystem.get(configuration);

        Random random = new Random();

        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, configuration, new Path("seqfile"), IntWritable.class, IntWritable.class);
        for(int i=0;i<100;i++) {
            writer.append(new IntWritable(random.nextInt(10000)), new IntWritable(i));
        }
        writer.close();

    }

    @Test
    public void read() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.25.164:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        SequenceFile.Reader seqfile = new SequenceFile.Reader(fileSystem, new Path("seqfile"), configuration);
        IntWritable key = new IntWritable();
        Text text = new Text();
        while (seqfile.next(key,text)){
            System.out.println(key.get()+":"+text.toString());
        }


    }

    @Test
    public void test(){
        Random random = new Random();
        for(int i=0;i<100;i++)
            System.out.println(random.nextInt(100));
    }
    public static void main(String args[]) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.25.164:8020");
        FileSystem fileSystem = FileSystem.get(configuration);

        Random random = new Random();

        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, configuration, new Path("seqfile"), IntWritable.class, IntWritable.class);
        for(int i=0;i<100;i++) {
            writer.append(new IntWritable(random.nextInt(10000)), new IntWritable(i));
        }
        writer.close();
    }


}
