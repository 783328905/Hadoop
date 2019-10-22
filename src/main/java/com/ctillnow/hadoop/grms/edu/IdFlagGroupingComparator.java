package com.ctillnow.hadoop.grms.edu;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/20 9:42
 * 4
 */
public class IdFlagGroupingComparator extends WritableComparator {
    public IdFlagGroupingComparator(){
        super(IdFlag.class,true);
    }
    @Override
    public int compare(WritableComparable a,WritableComparable b){
        IdFlag i1 = (IdFlag) a;
        IdFlag i2 = (IdFlag) b;
        return i1.getGid().compareTo(i2.getGid());
    }



}
