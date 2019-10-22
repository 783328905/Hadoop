package com.ctillnow.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Iterator;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/30 10:06
 * 4    自定义分区函数
 */
public class AllSort1 extends Configured implements Tool {



    public static void main(String args[]) throws Exception {
        ToolRunner.run(new AllSort1(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {

        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration,"allsort");
        job.setJarByClass(this.getClass());

        job.setMapperClass(AllSortMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(AllSortReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setPartitionerClass(Partional.class);
        job.setNumReduceTasks(2);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(configuration.get("inpath")));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(configuration.get("outpath")));
        job.waitForCompletion(true);

        return 0;
    }
    public static class AllSortMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String splits[] = value.toString().split("[\t]");
            context.write(new IntWritable(Integer.parseInt(splits[0])),new IntWritable(Integer.parseInt(splits[2])));
        }
    }
    public static class AllSortReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable > {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int num = Integer.MIN_VALUE;
            for (IntWritable i:values){
                num  = i.get()>num?i.get():num;
            }
            context.write(key,new IntWritable(num));
        }
    }
}
