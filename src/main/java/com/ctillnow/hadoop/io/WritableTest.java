package com.ctillnow.hadoop.io;

import org.apache.hadoop.io.IntWritable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/28 16:49
 * 4
 */
public class WritableTest {

    public static void main(String args[]) throws IOException {
        Integer i =new Integer(5);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(i);
        objectOutputStream.flush();
        byte ba[] = byteArrayOutputStream.toByteArray();

        System.out.println(byteArrayOutputStream.toByteArray().length);

        IntWritable intWritable = new IntWritable(i);
        ByteArrayOutputStream byteArrayOutputStream1 = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream1 = new ObjectOutputStream(byteArrayOutputStream1);
        intWritable.write(objectOutputStream);
        objectOutputStream.flush();
        byte ba1[] = byteArrayOutputStream1.toByteArray();
        System.out.println(ba1.length);

    }
}
