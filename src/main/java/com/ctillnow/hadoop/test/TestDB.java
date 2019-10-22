package com.ctillnow.hadoop.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/22 0:44
 * 4
 */
public class TestDB {
    public static void main(String args[]) throws Exception{
        String url ="jdbc:mysql://localhost:3306/grms";
        String username = "root";
        String password = "783328905" ;
        Class.forName("com.mysql.jdbc.Driver");
        
    }
}
