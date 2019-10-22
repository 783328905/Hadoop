package com.ctillnow.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/28 11:27
 * 4 hdfs高级操作 数据一致性(较验和)，压缩，
 */
public class Demo2 {

    @Test
    public void testBlock(){
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.25.164:8020");
        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            FSDataInputStream fsDataInputStream = fileSystem.open(new Path("hello.txt"));
            HdfsDataInputStream hdfsDataInputStream = (HdfsDataInputStream) fsDataInputStream;
            List<LocatedBlock> blocks = hdfsDataInputStream.getAllBlocks();

            blocks.forEach((i)->{System.out.println("-------------");
                ExtendedBlock b = i.getBlock();
                System.out.println(i.getBlockSize());
                DatanodeInfo[] locations = i.getLocations();
                System.out.println(b.getBlockId());
                System.out.println(b.getBlockName());
                for(DatanodeInfo l:locations){
                    System.out.println(l.getHostName());
                }


            });


        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testCompress(){
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.25.164:8020");
        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            File file = new File("hello.txt");
            FileInputStream inputStream = new FileInputStream(file);
            FSDataOutputStream outputStream = fileSystem.create(new Path("/hello.tar.gz"));

            //利用工厂模式获得compressCodec
            CompressionCodecFactory factory = new CompressionCodecFactory(configuration);
            CompressionCodec codec = factory.getCodec(new Path("/hello.tar.gz"));
            CompressionOutputStream compressionOutputStream = codec.createOutputStream(outputStream);
            IOUtils.copyBytes(inputStream,compressionOutputStream,128,true);
            return;



        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUnCompress(){
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.25.164:8020");

        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            File file = new File("abc.txt");
            FileOutputStream fileOutputStream = new FileOutputStream(file);

            FSDataInputStream inputStream = fileSystem.open(new Path("/hello.tar.gz"));
            CompressionCodecFactory factory = new CompressionCodecFactory(configuration);
            CompressionCodec codec = factory.getCodec(new Path("/hello.tar.gz"));
            CompressionInputStream compressionInputStream = codec.createInputStream(inputStream);

            IOUtils.copyBytes(compressionInputStream,fileOutputStream,128,true);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }





}
