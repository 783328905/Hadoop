package com.ctillnow.hadoop.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/30 10:18
 * 4
 */
public class Partional extends Partitioner<IntWritable,IntWritable> {

    @Override
    public int getPartition(IntWritable intWritable, IntWritable intWritable2, int i) {
        int y0 = intWritable.get()-10000;
       if(y0<50)
           return 0;
       else return 1;


    }
}
