package com.ctillnow.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;
import java.util.Queue;

/**
 * hadoop io常规操作
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/27 16:31
 * 4
 */
public class Demo1 {
    public FileSystem fileSystem = null;

    @Test
    public void getFile(){
        //1 获取配置对象
        //获取文件对象
        //选取合适流


        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.25.164:8020");
        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            System.out.println(fileSystem.getClass());
            FSDataInputStream open = fileSystem.open(new Path("/ihaveadream.txt"));
            int num = 0;
            byte [] buff = new byte[1024];
            while ((num=open.read(buff))!=-1)
            {
                System.out.println(new String(buff,0,num));
            }
            IOUtils.copyBytes(open,System.out,1024);




        } catch (IOException e) {
            e.printStackTrace();
        }






    }


    @Test
    public void test(){
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.25.164:8020");
        System.out.println(configuration);

        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            System.out.println(fileSystem);
            FSDataInputStream open = fileSystem.open(new Path("/ihaveadream.txt"));
            String line=null;
            while ((line=open.readLine())!=null) {
                System.out.println(line);
            }



        } catch (IOException e) {
            e.printStackTrace();
        }
        //新建出来的conf对象，实际是有配置信息
        //
    }
    @Test
    public void testWrite(){
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.25.164:8020");
        try {
             FileSystem fileSystem = FileSystem.get(configuration);
             //FSDataOutputStream fsDataOutputStream=fileSystem.append(new Path("/io.txt"));
             FSDataOutputStream fsDataOutputStream=fileSystem.create(new Path("/io.txt"),true);
             fsDataOutputStream.writeUTF("sssssssssssss");
            //fileSystem.delete(new Path("/io.txt"),true);



        } catch (IOException e) {
            e.printStackTrace();
        }


    }
    @Test
    public void testDownload(){
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.25.164:8020");
        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            DataInputStream inputStream = fileSystem.open(new Path("/ihaveadream.txt"));
            FileOutputStream outputStream =  new FileOutputStream(new File("hello.txt"));
            IOUtils.copyBytes(inputStream,outputStream,1024,true);

        } catch (IOException e) {
            e.printStackTrace();
        }


    }
    @Test
    public void testUpload(){
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.25.164:8020");
        try {
            fileSystem = FileSystem.get(configuration);
            FSDataOutputStream outputStream = fileSystem.create(new Path("/hello.txt"));
            InputStream inputStream= new FileInputStream(new File("hello.txt"));
            IOUtils.copyBytes(inputStream,outputStream,1024,true);

        } catch (IOException e) {
            e.printStackTrace();
        }



    }



    //递归输出目录下所有目录及子文件

    public void show(Path path) throws IOException {
        FileStatus fileStatus[] = fileSystem.listStatus(path);

        try {
            for (FileStatus status : fileStatus) {
                if (status.isFile()) {
                    System.out.println("文件"+status.getPath());
                } else {
                    System.out.println("目录"+status.getPath());
                    show(status.getPath());

                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void queueShow(Queue queue) throws IOException {


        while (queue.size()>0){
            FileStatus[] fileStatuses = fileSystem.listStatus((Path) queue.poll());
        for (FileStatus fileStatus1:fileStatuses)
        {
            if(fileStatus1.isFile()){
                System.out.println(fileStatus1.getPath());
                //showDetail(fileStatus1);
            }else{
                System.out.println(fileStatus1.getPath());
                queue.offer(fileStatus1.getPath());

            }



        }
        }


    }


    @Test
    public  void test1() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.25.164:8020");

            fileSystem = FileSystem.get(configuration);
            Path path  = new Path("/data");
            show(path);


    }
    @Test
    public  void testQueueShow() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.25.164:8020");

            fileSystem = FileSystem.get(configuration);
            Path path  = new Path("/data");
            Queue<Path> queue = new LinkedList<Path>();
            queue.offer(path);




            queueShow(queue);


    }
    public void showDetail(FileStatus status){
        System.out.println(status.getPath());
        System.out.println(status.getGroup());
        System.out.println(status.isFile());
        System.out.println(status.getPermission());
        System.out.println(status.getBlockSize());
        System.out.println(status.getOwner());



    }




}
