package com.ctillnow.hadoop.grms.edu;

import org.apache.hadoop.mapreduce.Partitioner;

import javax.xml.soap.Text;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/20 9:39
 * 4
 */
public class IdFlagPartitioner extends Partitioner<IdFlag,Text> {

    @Override
    public int getPartition(IdFlag idFlag, Text text, int i) {
        return Math.abs(idFlag.getGid()
                .toString().hashCode())%127;
    }
}
