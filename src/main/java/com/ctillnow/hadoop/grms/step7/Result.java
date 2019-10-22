package com.ctillnow.hadoop.grms.step7;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.db.DBWritable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/21 11:10
 * 4
 */
public class Result implements DBWritable,WritableComparable<Result> {
    private Text user_id = new Text();
    private Text good_id = new Text();
    private IntWritable res = new IntWritable();

    public Result(Text user_id, Text good_id, IntWritable res) {
        this.user_id = new Text(user_id.toString());
        this.good_id = new Text(good_id.toString());
        this.res = new IntWritable(res.get());
    }
    public Result(String user_id, String good_id, int res) {
        this.user_id = new Text(user_id);
        this.good_id = new Text(good_id);
        this.res = new IntWritable(res);
    }

    public Text getUser_id() {
        return user_id;
    }

    public void setUser_id(Text user_id) {
        this.user_id = user_id;
    }

    public Text getGood_id() {
        return good_id;
    }

    public void setGood_id(Text good_id) {
        this.good_id = good_id;
    }

    public IntWritable getRes() {
        return res;
    }

    public void setRes(IntWritable res) {
        this.res = res;
    }

    public Result() {
    }

    @Override
    public int compareTo(Result o) {
        return this.user_id.compareTo(o.user_id)==0?this.res.compareTo(o.res):(this.user_id.compareTo(o.user_id));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.user_id.write(dataOutput);
        this.good_id.write(dataOutput);
        this.res.write(dataOutput);


    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.user_id.readFields(dataInput);
        this.good_id.readFields(dataInput);
        this.res.readFields(dataInput);



    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,this.user_id.toString());
        preparedStatement.setString(2,good_id.toString());
        preparedStatement.setInt(3,this.res.get());

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        if(resultSet==null) {
            return;
        }
        this.user_id=new Text(resultSet.getString(1));
        this.good_id=new Text(resultSet.getString(2));
        this.res=new IntWritable(resultSet.getInt(3));



    }
}
