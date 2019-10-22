package com.ctillnow.hadoop.grms.step7;

import com.ctillnow.hadoop.grms.step1.PurchaseGoodsList;
import com.ctillnow.hadoop.grms.step2.GoodsConcurrenceList;
import com.ctillnow.hadoop.grms.step3.GoodsCooccurrenceMatrix;
import com.ctillnow.hadoop.grms.step4.UserBuyGoodsVector;
import com.ctillnow.hadoop.grms.step5.MultiplyGoodsMatrixAndUserVector;
import com.ctillnow.hadoop.grms.step6.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/26 20:10
 * 4
 */
public class ResultFlow extends Configured implements Tool {


    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path=new Path("/step1");
        if(fileSystem.exists(path)) {


            fileSystem.delete(new Path("/step1"), true);
            fileSystem.delete(new Path("/step2"), true);
            fileSystem.delete(new Path("/step3"), true);
            fileSystem.delete(new Path("/step4"), true);
            fileSystem.delete(new Path("/step5"), true);
            fileSystem.delete(new Path("/step6-1"), true);
            fileSystem.delete(new Path("/step6-2"), true);
            fileSystem.delete(new Path("/step6-3"), true);
            fileSystem.delete(new Path("/step7"), true);


        }
        //step1
        Job step1 = Job.getInstance(configuration,"step1");
        step1.setJarByClass(PurchaseGoodsList.class);
        step1.setMapperClass(PurchaseGoodsList.PGLMapper.class);
        step1.setMapOutputKeyClass(Text.class);
        step1.setMapOutputValueClass(Text.class);

        step1.setReducerClass(PurchaseGoodsList.PGLReducer.class);
        step1.setOutputKeyClass(Text.class);
        step1.setOutputValueClass(Text.class);
        step1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(step1,new Path("/matix"));
        step1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(step1,new Path("/step1"));

        //step2
        Job step2 = Job.getInstance(configuration,"step2");
        step2.setJarByClass(GoodsConcurrenceList.class);

        step2.setMapperClass(GoodsConcurrenceList.GoodsConcurrenceListMapper.class);
        step2.setMapOutputKeyClass(Text.class);
        step2.setMapOutputValueClass(IntWritable.class);
        step2.setReducerClass(GoodsConcurrenceList.GoodsConcurrenceListReducer.class);
        step2.setOutputKeyClass(Text.class);
        step2.setOutputValueClass(IntWritable.class);

        step2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(step2,new Path(("/step1")));
        step2.setOutputValueClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(step2,new Path(("/step2")));

        //step3
        Job step3 = Job.getInstance(configuration,"step3");
        step3.setJarByClass(GoodsCooccurrenceMatrix.class);

        step3.setMapperClass(GoodsCooccurrenceMatrix.GoodsCooccurrenceMatrixMapper.class);
        step3.setMapOutputKeyClass(Text.class);
        step3.setMapOutputValueClass(Text.class);

        step3.setReducerClass(GoodsCooccurrenceMatrix.GoodsCooccurrenceMatrixReducer.class);
        step3.setOutputKeyClass(Text.class);
        step3.setOutputValueClass(Text.class);

        step3.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(step3,new Path(("/step2")));

        step3.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(step3,new Path(("/step3")));

        //step4
        Job step4 = Job.getInstance(configuration,"step4");
        step4.setJarByClass(UserBuyGoodsVector.class);

        step4.setMapperClass(UserBuyGoodsVector.UserBuyGoodsVectorMapper.class);
        step4.setMapOutputKeyClass(Text.class);
        step4.setMapOutputValueClass(Text.class);

        step4.setReducerClass(UserBuyGoodsVector.UserBuyGoodsVectorReducer.class);
        step4.setOutputKeyClass(Text.class);
        step4.setOutputValueClass(Text.class);

        step4.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(step4,new Path(("/matix")));
        step4.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(step4,new Path(("/step4")));


        //step5
        Job step5 = Job.getInstance(configuration,"MultiplyGoodsMatrixAndUserVector");
        step5.setJarByClass(MultiplyGoodsMatrixAndUserVector.class);

        MultipleInputs.addInputPath(step5,new Path(("/step3")),TextInputFormat.class,MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorFirstMapper.class);
        MultipleInputs.addInputPath(step5,new Path(("/step4")),TextInputFormat.class,MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorSecondMapper.class);
        step5.setOutputKeyClass(Text.class);
        step5.setMapOutputValueClass(Text.class);


        step5.setReducerClass(MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorReducer.class);
        step5.setOutputKeyClass(Text.class);
        step5.setOutputValueClass(Text.class);

        step5.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(step5,new Path(("/step5")));



        

        //step6-2 求和
        Job step61 = Job.getInstance(configuration,"Sum");
        step61.setJarByClass(MakeSumForMultiplication.class);

        step61.setMapperClass(MakeSumForMultiplication.MakeSumForMultiplicationMapper.class);
        step61.setMapOutputKeyClass(Text.class);
        step61.setMapOutputValueClass(Text.class);

        step61.setReducerClass(MakeSumForMultiplication.MakeSumForMultiplicationReducer.class);
        step61.setOutputKeyClass(Text.class);
        step61.setOutputValueClass(IntWritable.class);

        step61.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(step61,new Path(("/step5")));
        step61.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(step61,new Path(("/step6-1")));


        //step6-2去重
        Job step62 = Job.getInstance(configuration,"DuplicateDataForResult");
        step62.setJarByClass(DuplicateDataForResult.class);

        MultipleInputs.addInputPath(step62,new Path(("/step1")),TextInputFormat.class,DuplicateDataForResult.DuplicateDataForResultFirstMapper.class);
        MultipleInputs.addInputPath(step62,new Path(("/step6-1")),TextInputFormat.class,DuplicateDataForResult.DuplicateDataForResultSecondMapper.class);
        step62.setMapOutputKeyClass(Text.class);
        step62.setOutputValueClass(Text.class);

        step62.setReducerClass(DuplicateDataForResult.DuplicateDataForResultReducer.class);
        step62.setOutputKeyClass(Text.class);
        step62.setOutputValueClass(Text.class);

        step62.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(step62,new Path(("/step6-2")));


        //step6-3 排序
        Job step63 = Job.getInstance(configuration,"resultsort");
        step63.setJarByClass(ResultSort.class);

        step63.setMapperClass(ResultSort.ResultSortMapper.class);
        step63.setMapOutputKeyClass(UidValue.class);
        step63.setMapOutputValueClass(Text.class);


        step63.setReducerClass(ResultSort.ResultSortReducer.class);
        step63.setOutputKeyClass(Text.class);
        step63.setOutputValueClass(Text.class);

        step63.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(step63,new Path(("/step6-2")));
        step63.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(step63,new Path(("/step6-3")));

        step63.setPartitionerClass(UidPartitioner.class);
        step63.setGroupingComparatorClass(UidGroupingComparator.class);

        //step7

        Job step7 = Job.getInstance(configuration,"toDB");
        step7.setJarByClass(ReslutToDB.class);

        step7.setMapperClass(ReslutToDB.ResultToDBMapper.class);
        step7.setMapOutputKeyClass(Result.class);
        step7.setMapOutputValueClass(NullWritable.class);

        step7.setReducerClass(ReslutToDB.ResultToDBReducer.class);
        step7.setOutputKeyClass(Result.class);
        step7.setOutputValueClass(NullWritable.class);
        //Class.forName("com.mysql.cj.jdbc.Driver");
        step7.setInputFormatClass(TextInputFormat.class);

        step7.setOutputFormatClass(DBOutputFormat.class);
        DBConfiguration.configureDB(step7.getConfiguration(),"com.mysql.cj.jdbc.Driver","jdbc:mysql://192.168.25.1:3306/grms?useSSL=true&serverTimezone=GMT","root","783328905");
        DBOutputFormat.setOutput(step7,"hj_ctillnow_grms","user_id","good_id","res");
        TextInputFormat.addInputPath(step7,new Path(("/step6-3")));

        //装配工作流
        ControlledJob step1_cj = new ControlledJob(step1.getConfiguration());
        ControlledJob step2_cj= new ControlledJob(step2.getConfiguration());
        ControlledJob step3_cj = new ControlledJob(step3.getConfiguration());
        ControlledJob step4_cj = new ControlledJob(step4.getConfiguration());
        ControlledJob step5_cj = new ControlledJob(step5.getConfiguration());
        ControlledJob step61_cj = new ControlledJob(step61.getConfiguration());
        ControlledJob step62_cj = new ControlledJob(step62.getConfiguration());
        ControlledJob step63_cj = new ControlledJob(step63.getConfiguration());
        ControlledJob step7_cj= new ControlledJob(step7.getConfiguration());

        step2_cj.addDependingJob(step1_cj);
        step3_cj.addDependingJob(step2_cj);
        step4_cj.addDependingJob(step3_cj);
        step5_cj.addDependingJob(step4_cj);
        step61_cj.addDependingJob(step5_cj);
        step62_cj.addDependingJob(step61_cj);
        step63_cj.addDependingJob(step62_cj);
        step7_cj.addDependingJob(step63_cj);
        JobControl jobControl = new JobControl("grms");
        jobControl.addJob(step1_cj);
        jobControl.addJob(step2_cj);
        jobControl.addJob(step3_cj);
        jobControl.addJob(step4_cj);
        jobControl.addJob(step5_cj);
        jobControl.addJob(step61_cj);
        jobControl.addJob(step62_cj);
        jobControl.addJob(step63_cj);
        jobControl.addJob(step7_cj);

        Thread t = new Thread(jobControl);
        t.start();
        while(!jobControl.allFinished()) {
        }
        System.exit(0);


        return 0;
    }
    public static void main(String args[]) throws Exception {
        ToolRunner.run(new ResultFlow(),args);


    }
}
