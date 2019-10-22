package com.ctillnow.hadoop.grms.step6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/20 11:08
 * 4
 */
public class UidPartitioner extends Partitioner<UidValue,Text > {


    @Override
    public int getPartition(UidValue uidValue, Text text, int i) {
        return Math.abs(uidValue.getUid().get()*127)%i;
    }
}
