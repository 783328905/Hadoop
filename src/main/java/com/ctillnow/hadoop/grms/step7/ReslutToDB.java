package com.ctillnow.hadoop.grms.step7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/20 14:45
 * 4
 */
public class ReslutToDB extends Configured implements Tool {

    public static void main(String args[])throws Exception{
        ToolRunner.run(new ReslutToDB(),args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration,"toDB");
        job.setJarByClass(this.getClass());

        job.setMapperClass(ResultToDBMapper.class);
        job.setMapOutputKeyClass(Result.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(ResultToDBReducer.class);
        job.setOutputKeyClass(Result.class);
        job.setOutputValueClass(NullWritable.class);
        //Class.forName("com.mysql.cj.jdbc.Driver");
        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputFormatClass(DBOutputFormat.class);
        DBConfiguration.configureDB(job.getConfiguration(),"com.mysql.cj.jdbc.Driver","jdbc:mysql://192.168.25.1:3306/grms?useSSL=true&serverTimezone=GMT","root","783328905");
        DBOutputFormat.setOutput(job,"hj_ctillnow_grms","user_id","good_id","res");

        //DistributedCache.addFileToClassPath(new Path("/lib/mysql/mysql-connector-java-5.1.46-bin.jar"),configuration);
        //job.addArchiveToClassPath(new Path("hdfs://192.168.25.164:8020/lib/mysql/mysql-connector-java-5.1.32.jar"));

        TextInputFormat.addInputPath(job,new Path(configuration.get("inpath")));

        job.waitForCompletion(true);



        return 0;
    }
    public static class ResultToDBMapper extends Mapper<LongWritable,Text,Result,NullWritable> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String infos[] = value.toString().split("[\t]");
            Result result = new Result(new Text(infos[0]),new Text(infos[1]),new IntWritable(Integer.parseInt(infos[2])));
            context.write(result,NullWritable.get());
        }

    }
    public static class ResultToDBReducer extends Reducer<Result,NullWritable,Result,NullWritable>{
        @Override
        protected void reduce(Result key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }
}
