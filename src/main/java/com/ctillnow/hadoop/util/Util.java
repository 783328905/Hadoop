package com.ctillnow.hadoop.util;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/30 9:51
 * 4
 */
public class Util {
    public static String getHostName(){
        try {
            return InetAddress.getLocalHost().getHostName();


        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;

    }

    public static int getPID(){
        try {
            String info = ManagementFactory.getRuntimeMXBean().getName();
            return Integer.parseInt(info.substring(0,info.indexOf("0")));
        } catch (Exception e){
            e.printStackTrace();
        }
        return 0;

    }

    public static String getTID(){
        try {
            return Thread.currentThread().getName();
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static String getObjectInfo(Object o){
        try {
            o.getClass().getSimpleName();
            return o.toString();
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }



}


