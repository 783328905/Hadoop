package com.ctillnow.hadoop.grms.edu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/20 9:29
 * 4
 */
//
public class IdFlag implements WritableComparable<IdFlag> {
    private Text gid = new Text();
    private IntWritable flag = new IntWritable();

    public IdFlag(Text gid, IntWritable flag) {
        this.gid = new Text(gid.toString());
        this.flag = new IntWritable(flag.get());
    }

    public Text getGid() {
        return gid;
    }

    @Override
    public int compareTo(IdFlag o) {
        return gid.compareTo(o.gid)==0?
                this.flag.compareTo(o.flag):
                this.gid.compareTo(o.gid);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        gid.write(dataOutput);
        flag.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        gid.readFields(dataInput);
        flag.readFields(dataInput);
    }
}
