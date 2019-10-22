package com.ctillnow.hadoop.grms.step7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/21 20:58
 * 4
 */
public class DBToHdfs extends Configured implements Tool {


    public static void main(String args[])throws Exception{
        ToolRunner.run(new DBToHdfs(),args);

    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration,"DBToHdfs");
        job.setJarByClass(this.getClass());

        job.setMapperClass(DBToHdfsMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Result.class);

        //job.addArchiveToClassPath(new Path("hdfs://192.168.25.164:8020/lib/mysql/mysql-connector-java-5.1.32.jar"));

        DBConfiguration.configureDB(job.getConfiguration(),"com.mysql.jdbc.Driver","jdbc:mysql://192.168.25.1:3306/grms?useSSL=true&serverTimezone=GMT","root","783328905");
        DBInputFormat.setInput(job,Result.class,"hj_ctillnow_grms","res>1000","res","user_id","good_id","res");
        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(configuration.get("outpath")));
        job.waitForCompletion(true);

        return 0;
    }
    public static class DBToHdfsMapper extends Mapper<LongWritable,Result,LongWritable,Result>{
        @Override
        protected void map(LongWritable key, Result value, Context context) throws IOException, InterruptedException {

            context.write(key,value);
        }
    }
}
