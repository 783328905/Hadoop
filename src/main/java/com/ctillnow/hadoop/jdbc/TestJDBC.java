package com.ctillnow.hadoop.jdbc;

import com.ctillnow.hadoop.grms.step6.ResultSort;
import com.ctillnow.hadoop.grms.step7.Result;
import com.google.common.annotations.VisibleForTesting;
import lombok.ToString;
import org.junit.jupiter.api.Test;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.sql.*;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/24 14:18
 * 4
 */
public class TestJDBC {
    public static final String URL= "jdbc:mysql://localhost:3306/grms?useSSL=true&serverTimezone=GMT";
    public static final String username = "root";
    public static final String password = "783328905";
    @Test
    public void testInsert() throws Exception{

        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection(URL,username,password);
        String sql = "insert into hj_ctillnow_grms values(?,?,?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1,"783328905");
        statement.setString(2,"cai");
        statement.setInt(3,22);
        if(statement.executeUpdate()>0) {
            System.out.println("插入数据完成！");

        }else {
            System.out.println("失败了");
        }



    }
    @Test
    public void testDel() throws Exception{
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection(URL,username,password);
        String sql = "delete  from hj_ctillnow_grms where user_id = 783328905";
        PreparedStatement statement = connection.prepareStatement(sql);
        if(statement.executeUpdate()>0){
            System.out.println("删除成功");
        }
        connection.close();
    }
    @Test
    public void testUpdate() throws Exception{
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection(URL,username,password);
        String sql = "update hj_ctillnow_grms set res =30 where user_id = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1,"783328905");
        if(preparedStatement.executeUpdate()>0)
            System.out.println("success");
        connection.close();
    }


    @Test
    public void getResult() throws Exception{
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection(URL,username,password);
        //String sql = "select * from hj_ctillnow_grms where user_id=? limit 0,3";
        String sql = "select * from hj_ctillnow_grms ";
        PreparedStatement statement = connection.prepareStatement(sql);
        //statement.setString(1,"10001");
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()){
            System.out.println(resultSet.getString(1)+"物品id:"+resultSet.getString(2)+"推荐结果"+resultSet.getInt(3));
        }
        connection.close();
    }

    @Test
    public void TestBantch()throws Exception{

        long start = System.currentTimeMillis();
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection  = DriverManager.getConnection(URL,username,password);
        String sql = "insert into user(name,tag,age) values(?,?,?)";
        connection.setAutoCommit(false);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        int batchCount = 0;

        for(int i=0;i<100000;i++){
            preparedStatement.setString(1,"cai"+i);
            preparedStatement.setString(2,"brive"+i);
            preparedStatement.setInt(3,30);

            preparedStatement.addBatch();
            batchCount++;

            if(batchCount==100) {
                batchCount =0;
                preparedStatement.executeBatch();
            }

        }
        if(preparedStatement!=null){
            preparedStatement.executeBatch();
        }
        connection.commit();

        connection.close();
        preparedStatement.close();


        long end = System.currentTimeMillis();

        System.out.println(end-start+"毫秒");



    }
    @Test
    public void testSave() throws Exception{
        //创建存储过程
        //create procedure sp_batchinsert(int n int)
        //insert into name value

        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection  = DriverManager.getConnection(URL,username,password);
        String sql = "insert into user(name,tag,age) values(?,?,?)";
        connection.setAutoCommit(false);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        //connection.prepareCall()
        connection.commit();

        connection.close();
        preparedStatement.close();
    }

    @Test
    public void test() throws ClassNotFoundException, SQLException {

        int batch =0;
       Class.forName("com.mysql.jdbc.Driver");
       Connection connection  = DriverManager.getConnection(URL,username,password);
       connection.setAutoCommit(false);
       String sql = "select * from hj_ctillnow_grms";
       PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()){
           System.out.println("user"+resultSet.getString("user_id")+","+resultSet.getString("good_id")+","+
           resultSet.getInt("res"));

        }



    }

    @Test
    public void test1() throws Exception{
        long start = System.currentTimeMillis();

        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection(URL,username,password);
        String sql  = "insert into user(name,tag,age) values(?,?,?)";
        int batchCount =0;
        connection.setAutoCommit(false);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        for(int i =0;i<10000;i++){
            preparedStatement.setString(1,"name"+i);
            preparedStatement.setString(2,"tag"+i);
            preparedStatement.setInt(3,i);
            preparedStatement.addBatch();
            batchCount++;
            if(batchCount==100){
                batchCount=0;
                preparedStatement.executeBatch();
            }
        }
        if(preparedStatement!=null){
            preparedStatement.executeBatch();
        }
        connection.commit();
        connection.close();
        preparedStatement.close();
        long end = System.currentTimeMillis();
        System.out.println((end-start)/1000.0+"秒");
    }
}
