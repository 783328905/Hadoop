package com.ctillnow.hadoop.grms.step6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.time.temporal.Temporal;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/19 19:22
 * 4
 */
public class MakeSumForMultiplication extends Configured implements Tool {
    public static void main(String args[])throws Exception{
        ToolRunner.run(new MakeSumForMultiplication(),args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf,"Sum");
        job.setJarByClass(MakeSumForMultiplication.class);

        job.setMapperClass(MakeSumForMultiplicationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MakeSumForMultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(conf.get("inpath")));
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(conf.get("outpath")));

        job.waitForCompletion(true);
        return 0;
    }
    public static class MakeSumForMultiplicationMapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String  [] infos =line.split("[\t]");
            context.write(new Text(infos[0]),new Text(infos[1]));
        }
    }
    public static class MakeSumForMultiplicationReducer extends Reducer<Text,Text,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text value:values){
                sum += Integer.parseInt(value.toString());
            }

            context.write(key,new IntWritable(sum));
        }
    }
}
