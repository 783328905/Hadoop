/*
package com.ctillnow.hadoop.test;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

*/
/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/19 18:35
 * 4
 *//*

public class TestList
{
    public void tests(){
        String s[] = null;
        String str = "ss ssss";
        s = str.substring(1).split("[ ]");
        for (int i = 0 ;i<s.length;i++)
            System.out.println(s[i]);


    }
    @Test
    public void tests1(){
        //10001 120001
        //10001 120002
        //10001 120003

        //10001 620001  3
        //10001 620002  4
        List<String> list1 = new ArrayList<String>();
        List<String> list2 = new ArrayList<String>();
        list1.add("20001");
        list1.add("20002");
        list2.add("20001\t3");
        list2.add("20003\t4");
        Map<String,String> map = new HashMap<String , String >();
        map.put("20001","1");
        map.put("20003","1");
        for (String s:list1)
            map.remove(s);
        map.forEach((k,v)->System.out.println(k+":"+v));
        */
/*for (String s1 : list1)
            for (String s2 : list2){
                if (s1.equals(s2.split("[\t]")[0]))
                     list2.remove(s2);

            }*//*



        //list2.forEach(System.out :: println);



    }
}
*/
