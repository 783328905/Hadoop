package com.ctillnow.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/30 10:58
 * 4    自定义分区函数，设置分区区间
 *      通过采样器生成分区函数
 *      好坑的全排序
 *
 *      5374    0
 *      234     1
 *      9496    2
 *      7211    3
 *      8753    4
 *      6517    5
 */
public class AllSort2 extends Configured implements Tool {


    public static void main(String args[]) throws Exception {
        ToolRunner.run(new AllSort2(),args);
    }
    @Override
    public int run(String[] strings) throws Exception {

        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration,"allsort2");
        job.setJarByClass(this.getClass());

        job.setMapperClass(AllSortMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(AllSortReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setNumReduceTasks(2);
        SequenceFileInputFormat.addInputPath(job,new Path(configuration.get("inpath")));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(configuration.get("outpath")));

        //创建随机采样对象
        //freq 每个key被选中的概率
        //numSample：抽取样本总数
        //maxSpliteSampled:最大采样切片数
        job.setPartitionerClass(TotalOrderPartitioner.class);

        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),new Path("_partitions"));

        //job.setPartitionerClass(Partional.class);

        InputSampler.Sampler<IntWritable,IntWritable>sampler= new InputSampler.RandomSampler<IntWritable, IntWritable>(0.1,10000,10);
        InputSampler.writePartitionFile(job,sampler);




        job.waitForCompletion(true);

        return 0;
    }
    public static class AllSortMapper extends Mapper<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
        protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {


            context.write(key,value);
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
