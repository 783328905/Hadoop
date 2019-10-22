package com.ctillnow.hadoop.io;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/28 17:02
 * 4
 */
public class Student implements WritableComparable {

    public IntWritable id = new IntWritable();
    private Text name = new Text();
    private DoubleWritable weight =new DoubleWritable();

    public Student(){}

    public IntWritable getId() {
        return id;
    }

    public void setId(IntWritable id) {
        this.id = new IntWritable(id.get());
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = new Text(name.toString());
    }

    public DoubleWritable getWeight() {
        return weight;
    }

    public void setWeight(DoubleWritable weight) {
        this.weight = new DoubleWritable(weight.get());
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        id.write(dataOutput);
        name.write(dataOutput);
        weight.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id.readFields(dataInput);
        name.readFields(dataInput);
        weight.readFields(dataInput);

    }
}
