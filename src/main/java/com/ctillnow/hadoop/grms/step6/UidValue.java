package com.ctillnow.hadoop.grms.step6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/19 22:30
 * 4    用户id 推荐值
 */
public class UidValue implements WritableComparable<UidValue> {
    private IntWritable uid = new IntWritable();
    private IntWritable value = new IntWritable();

    public UidValue(IntWritable uid, IntWritable value) {
        this.uid = new IntWritable(uid.get());
        this.value = new IntWritable(value.get());
    }
    public UidValue(){};

    public IntWritable getUid() {
        return uid;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        uid.write(dataOutput);
        value.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        uid.readFields(dataInput);
        value.readFields(dataInput);

    }

    @Override
    public int compareTo(UidValue o) {
        return this.uid.compareTo(o.uid)==0?this.value.compareTo(o.value):this.uid.compareTo(o.uid);
    }

    public IntWritable getValue() {
        return value;
    }
}
