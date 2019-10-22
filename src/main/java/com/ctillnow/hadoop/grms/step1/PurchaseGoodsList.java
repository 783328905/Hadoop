package com.ctillnow.hadoop.grms.step1;

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
 * 3 * @Date: 2019/6/18 9:41
 * 4
 * 10001 20001 1
 * 10001 20001 1
 * 10003 20001 1
 * 10004 20001 1
 * Tool命令行传参
 */
public class PurchaseGoodsList extends Configured implements Tool {
    /**
     * yarn jar HadoopReview.jar com.ctillnow.hadoop.grms.step1.PurchaseGoodsList  -D inpath=/data/rmc/process/matrix_data.txt -D outpath=./grms/step1
     */
    public static void main(String []args)throws Exception{

        ToolRunner.run(new PurchaseGoodsList(),args);



    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration,"till_Pur");
        job.setJarByClass(this.getClass());

        job.setMapperClass(PGLMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(PGLReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(configuration.get("inpath")));
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(configuration.get("outpath")));
        job.waitForCompletion(true);
        return 0;
    }

    public static class PGLMapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String infos[] = value.toString().split("[ ]");
            context.write(new Text(infos[0]),new Text(infos[1]));
        }
    }
    public static class PGLReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer stringBuffer = new StringBuffer();
            values.forEach((i)->stringBuffer.append(i).append(","));
            String str = stringBuffer.substring(0,stringBuffer.length()-1);
            context.write(key,new Text(str));


        }
    }

}
