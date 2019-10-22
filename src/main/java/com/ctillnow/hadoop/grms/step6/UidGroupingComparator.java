package com.ctillnow.hadoop.grms.step6;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/20 11:16
 * 4
 */
public class UidGroupingComparator extends WritableComparator {
    public UidGroupingComparator(){
        super(UidValue.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        UidValue u1 = (UidValue) a;
        UidValue u2 = (UidValue) b;
        return u1.getUid().compareTo(u2.getUid());

    }
}
