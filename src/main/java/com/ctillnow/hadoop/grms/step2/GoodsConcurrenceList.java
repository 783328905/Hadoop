package com.ctillnow.hadoop.grms.step2;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/18 14:16
 * 4
 */

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
import java.util.ArrayList;
import java.util.List;

/**
 *计算两两商品的共线次数
 * 输入数据 /grms/step1
 * 输出数据 /grms/step2
 * 10001 20001 20002 20003
 * 20001 20002 4
 * 20002 20004 6
 * 下一步将共现列表整理成矩阵
 *
 *
 **/
public class GoodsConcurrenceList extends Configured implements Tool {


    public static void main(String args[]) throws Exception {
        ToolRunner.run(new GoodsConcurrenceList(),args);

    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf,"till-step2");
        job.setJarByClass(GoodsConcurrenceList.class);

        job.setMapperClass(GoodsConcurrenceListMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(GoodsConcurrenceListReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(conf.get("inpath")));
        job.setOutputValueClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(conf.get("outpath")));
        job.waitForCompletion(true);



        return 0;
    }
    public static class GoodsConcurrenceListMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String infos[] = value.toString().split("\t");
            String strings[] = infos[1].split(",");
            List <String> list = new ArrayList<String>();
            for(int i=0;i<strings.length;i++) {
                for (int j = 0; j < strings.length; j++){
                    list.add(strings[i]+","+strings[j]);
                }
            }
            for(int i=0;i<list.size();i++) {
                context.write(new Text(list.get(i)),new IntWritable(1));
            }

        }
    }
    public static class GoodsConcurrenceListReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Integer sum = 0;
            for(IntWritable value : values){
                sum += value.get();

            }

            context.write(key,new IntWritable(sum));

        }
    }
}
