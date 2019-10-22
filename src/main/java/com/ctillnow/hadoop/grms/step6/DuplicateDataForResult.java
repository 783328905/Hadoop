package com.ctillnow.hadoop.grms.step6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/19 19:43
 * 4
 */
public class DuplicateDataForResult extends Configured implements Tool {

    public static void main(String args[])throws Exception{
        ToolRunner.run(new DuplicateDataForResult(),args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration,"DuplicateDataForResult");
        job.setJarByClass(DuplicateDataForResult.class);

        MultipleInputs.addInputPath(job,new Path(configuration.get("inpath1")),TextInputFormat.class,DuplicateDataForResultFirstMapper.class);
        MultipleInputs.addInputPath(job,new Path(configuration.get("inpath2")),TextInputFormat.class,DuplicateDataForResultSecondMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setReducerClass(DuplicateDataForResultReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(configuration.get("outpath")));

        job.waitForCompletion(true);
        return 0;
    }
    //./grms/step1
    /*10001	20001,20005,20006,20007,20002
            10002	20006,20003,20004*/
    public static class DuplicateDataForResultFirstMapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str[]= value.toString().split("[\t]");
            String  [] strings = str[1].split(",");
            for(String s: strings) {
                context.write(new Text(str[0]), new Text("1"+s));
            }
        }
    }
    //./grms/step6/sum
    /*10085,20107     3
      10058,20107     3
      10004,20107     3
      10063,20107     3
      10028,20107     3*/

    public static class DuplicateDataForResultSecondMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str[] = value.toString().split("\t");
            String strings [] = str[0].split(",");
            context.write(new Text(strings[0]),new Text("6"+strings[1]+"\t"+str[1]));

        }




    }
    //10001 120001
    //10001 120002
    //10001 120003

    //10001 620001  3
    //10001 620002  4
    public static class DuplicateDataForResultReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> step1List = new ArrayList<String>();
            List<String> step6List = new ArrayList<String>();
            for (Text value : values) {
                String str = value.toString();
                if (str.charAt(0) == '1') {
                    step1List.add(str.substring(1));
                }
                if (str.charAt(0) == '6') {
                    step6List.add(str.substring(1));
                }
            }
            Map<String ,String> hashMap = new HashMap<String,String>();

            for (String s2 : step6List){

                hashMap.put(s2.split("[\t]")[0],s2.split("[\t]")[1]);
            }
            for (String s1 : step1List) {
                hashMap.remove(s1);
            }
            hashMap.forEach((k,v)-> {
                try {
                    context.write(key,new Text(k+"\t"+v));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }


    }


}
