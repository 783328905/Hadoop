package com.ctillnow.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
 * 3 * @Date: 2019/6/20 16:11
 * 4
 */
public class AllSort extends Configured implements Tool {

    public static void main(String args[])throws Exception{
        ToolRunner.run(new AllSort(),args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration,"allsort");
        job.setJarByClass(this.getClass());

        job.setMapperClass(AllSortMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        //job.setReducerClass(AllSortReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(configuration.get("inpath")));
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(configuration.get("outpath")));

        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
        return 0;
    }

    public static class AllSortMapper extends Mapper<LongWritable,Text,IntWritable,NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(Integer.parseInt(value.toString())),NullWritable.get());
        }
    }
    public static class AllSortReducer extends Reducer<IntWritable,NullWritable,IntWritable,NullWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

}
