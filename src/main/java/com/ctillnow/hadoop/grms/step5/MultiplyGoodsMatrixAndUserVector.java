package com.ctillnow.hadoop.grms.step5;

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
import java.util.List;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/19 9:32
 * 4计算对于某个用户推荐某个商品的推荐值
 * 输入 step3 step4
 * 进行reduce端连接
 * 借助MultipleINputs把两个mapper输出
 *
 */
public class MultiplyGoodsMatrixAndUserVector extends Configured implements Tool {

    public static void main(String args[]) throws Exception{
        ToolRunner.run(new MultiplyGoodsMatrixAndUserVector(),args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration,"MultiplyGoodsMatrixAndUserVector");
        job.setJarByClass(MultiplyGoodsMatrixAndUserVector.class);

        MultipleInputs.addInputPath(job,new Path(configuration.get("inpath1")),TextInputFormat.class,MultiplyGoodsMatrixAndUserVectorFirstMapper.class);
        MultipleInputs.addInputPath(job,new Path(configuration.get("inpath2")),TextInputFormat.class,MultiplyGoodsMatrixAndUserVectorSecondMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(MultiplyGoodsMatrixAndUserVectorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(configuration.get("outpath")));
        job.waitForCompletion(true);

        return 0;
    }
    //物品相似矩阵20499   20000:2,20001:1,20002:2,20003:3,20004:1,20005:2,20006:4,20007:4
    //用户购买向量20497   10089:1,10038:1,10032:1
    //保证key一样
    public static class MultiplyGoodsMatrixAndUserVectorFirstMapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String infos[] = value.toString().split("[\t]");
            context.write(new Text(infos[0]),new Text("m"+infos[1]));



        }
    }
    public static class MultiplyGoodsMatrixAndUserVectorSecondMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String infos[] = value.toString().split("[\t]");
            context.write(new Text(infos[0]),new Text("v"+infos[1]));

        }
    }
    public static class MultiplyGoodsMatrixAndUserVectorReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //20000:2,20001:1,20002:2,20003:3,20004:1,20005:2,20006:4,20007:4
            //sm ->20000:2
            //sv ->10089:1
            List<String> sm = new ArrayList<String>();
            List<String> sv = new ArrayList<String>();


            for (Text value: values){
                String str = value.toString();
                if(str.charAt(0)=='m'){
                    String s1[] = str.substring(1).split(",");
                    for (String s2: s1)
                        sm.add(s2);


                }
                if(str.charAt(0)=='v'){
                    String s3[] = str.substring(1).split(",");
                    for (String s4:s3)
                        sv.add(s4);

                }


            }
            for (String sm1:sm) {
                for (String sv1:sv) {
                    String smm[] = sm1.split(":");
                    String svv[] = sv1.split(":");
                    context.write(new Text(svv[0]+","+smm[0]),
                            new Text(Integer.parseInt(smm[1])*Integer.parseInt(svv[1])+""));
                }
            }



        }
    }
}
