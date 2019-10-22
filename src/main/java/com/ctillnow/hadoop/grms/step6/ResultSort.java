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

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/19 21:54
 * 输入：172.16.0.4:16010 webpage_aliyun
 * zk 172.16.0.4 ,5,6,7
 * 输出 名字_类名
 * 数据清洗
 * f:st ==2
 * 从提取有效字段（url,title,il,ol）
 *
 * 4
 */
public class ResultSort extends Configured implements Tool {
    public static void main(String args[])throws Exception{
        ToolRunner.run(new ResultSort(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration,"resultsort");
        job.setJarByClass(ResultSort.class);

        job.setMapperClass(ResultSortMapper.class);
        job.setMapOutputKeyClass(UidValue.class);
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(ResultSortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(configuration.get("inpath")));
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(configuration.get("outpath")));

        job.setPartitionerClass(UidPartitioner.class);
        job.setGroupingComparatorClass(UidGroupingComparator.class);
        job.waitForCompletion(true);
        return 0;
    }
    public static class ResultSortMapper extends Mapper<LongWritable,Text,UidValue,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //10002 20002	2
            //10002 20002	1
            String infos[] = value.toString().split("[\t]");
            UidValue uidValue = new UidValue(new IntWritable(Integer.parseInt(infos[0])),new IntWritable(Integer.parseInt(infos[2])));
            context.write(uidValue,new Text(infos[1]));


        }
    }
    public static class ResultSortReducer extends Reducer<UidValue,Text,Text,Text>{
        @Override
        protected void reduce(UidValue key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values){
                context.write(new Text(key.getUid().get()+"\t"+value),new Text(key.getValue().get()+""));
            }

        }
    }
}
