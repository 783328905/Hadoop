package com.ctillnow.hadoop.grms.edu;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Iterator;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/20 9:48
 * 4
 */
public class MatrixVetor extends Configured implements Tool {


    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }
    public static class ReadGCMatrixMapper extends Mapper<Text,Text,IdFlag,Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            IdFlag idFlag = new IdFlag(key,new IntWritable(0));
            context.write(idFlag,value);
        }
    }
    public static class ReadPGVetorMapper extends Mapper<Text,Text,IdFlag,Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            IdFlag idFlag = new IdFlag(key,new IntWritable(1));
            context.write(idFlag,value);
        }
    }
    public static class GRReducer extends Reducer<IdFlag,Text,Text,Text>{
        @Override
        protected void reduce(IdFlag key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            //物品相似度
            Text gc = iterator.next();
            Text up = iterator.next();
            String ups[] = up.toString().split("[,]");
        }
    }

}
