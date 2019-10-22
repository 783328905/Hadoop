package com.ctillnow.hadoop.grms.step4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/18 15:29
 * 4
 */
public class UserBuyGoodsVector extends Configured implements Tool {

    public static void main(String args[]) throws Exception{
        ToolRunner.run(new UserBuyGoodsVector(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration,"step4");
        job.setJarByClass(this.getClass());

        job.setMapperClass(UserBuyGoodsVectorMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(UserBuyGoodsVectorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(configuration.get("inpath")));
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(configuration.get("outpath")));

        job.waitForCompletion(true);
        return 0;
    }
    //10001  20002,20003,20005,20006
    //map 20001 10001 1 20002 10001 1 20003 10001 1
    //re1 20001 10001 2  20004 10001 3
    //re2 20001  10001:3 10002:4
    public static class UserBuyGoodsVectorMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //10066 20015 1
            String infos[] = value.toString().split("[ ]");
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append(infos[0]).append(":").append(infos[2]);
            context.write(new Text(infos[1]),new Text(stringBuffer.toString()));


        }
    }

    public static class UserBuyGoodsVectorReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer stringBuffer = new StringBuffer();
            values.forEach((i)->stringBuffer.append(i).append(","));
            String str = stringBuffer.substring(0,stringBuffer.length()-1);
            context.write(key,new Text(str));
        }
    }
}
