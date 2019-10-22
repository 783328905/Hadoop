package com.ctillnow.hadoop.grms.step3;

import com.ctillnow.hadoop.grms.step2.GoodsConcurrenceList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/18 15:03
 * 4
 */
public class GoodsCooccurrenceMatrix extends Configured implements Tool {


    public static void main(String args[]) throws Exception{
        ToolRunner.run(new GoodsCooccurrenceMatrix(),args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf,"till-step3");
        job.setJarByClass(GoodsCooccurrenceMatrix.class);

        job.setMapperClass(GoodsCooccurrenceMatrixMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(GoodsCooccurrenceMatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(conf.get("inpath")));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(conf.get("outpath")));
        job.waitForCompletion(true);
        return 0;
    }
    public static class GoodsCooccurrenceMatrixMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //20125,20122     3
            String infos[] = value.toString().split("\t");
            String keysplit[] = infos[0].split(",");
            //20125 20122:3
            context.write(new Text(keysplit[0]),new Text(keysplit[1]+":"+infos[1]));


        }

    }
    public static class GoodsCooccurrenceMatrixReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer stringBuffer = new StringBuffer();
            values.forEach((i)->stringBuffer.append(i).append(","));
            String str = stringBuffer.substring(0,stringBuffer.length()-1);
            context.write(key,new Text(str));
        }
    }
}
