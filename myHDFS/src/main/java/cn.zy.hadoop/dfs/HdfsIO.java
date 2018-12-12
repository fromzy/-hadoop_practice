package cn.zy.hadoop.dfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author: zhang ying
 * @Date: 2018/12/4 21:01
 * @Version 1.0
 */
public class HdfsIO {
/*
* 1.获取文件系统
* 2.获取输入流
* 3.获取输出流
* 4.流的对拷
* 5.关闭资源
*
* */
    Configuration conf = null ;
    FileSystem fs =null;
   //上传文件的流
    FileInputStream fis = null;
    FSDataOutputStream fdos = null;
   //下载文件的流
    FSDataInputStream fdis = null;
    FileOutputStream fos = null;

    @Before//1.获取文件系统
    public void initHdfs() throws URISyntaxException, IOException, InterruptedException {
        conf = new Configuration();
        fs = FileSystem.get(new URI("hdfs://hadoop101:9000"),conf,"zy");
        System.out.println("----------------------------获取hdfs完毕-----------");
    }


    @Test//上传
    public void putFileToHdfs() throws IOException {
        /*
        输入流输出流都是相对于此程序而言的，
        读取本地文件是输入流，
        把读取到的文件输出到hdfs,是输出流，
        */
        //2.获取输入流；
        fis = new FileInputStream(new File("D:/UUUUUUUUUUUU/linux/jdk-8u181-linux-x64.tar.gz"));

        //3.获取输出流
        fdos = fs.create(new Path("/user/admin/data/jdk-8u181-linux-x64.tar.gz"));//把文件放到hdfs的目录，新名字

        //4.流的拷贝
        IOUtils.copyBytes(fis,fdos,conf);
    }

    @Test//下载文件
    public void  getFileFromHDFS() throws IOException {
        //2.获取输入流；
        //从hdfs输入到这里，
        fdis= fs.open(new Path("/user/admin/data/jdk-8u181-linux-x64.tar.gz"));

        //3.获取输出流
        //把从hdfs获取到的数据 输出 到本地，
        fos =new FileOutputStream(new File("d:/mytest/jdk-8u181-linux-x64.tar.gz"));

        //4.流的对拷
        IOUtils.copyBytes(fdis,fos,conf);

    }



    @Test
    public void putfile() throws IOException {

        //2.获取输入流；
        fis = new FileInputStream(new File("D:/mytest/b.txt"));

        //3.获取输出流
        fdos = fs.create(new Path("/user/admin/data/3aa.txt"));//把文件放到hdfs的目录，新名字

        //4.流的拷贝
        IOUtils.copyBytes(fis,fdos,conf);
    }






    @After//关闭资源
    public void closeHdfs() throws IOException {

        if(fs != null){
            fs.close();
        }

        //关闭上传文件的流
        if(fis != null){
            IOUtils.closeStream(fis);
        }

        if(fdos != null){
            IOUtils.closeStream(fdos);
        }

        //关闭下载文件的流
        if(fdis != null){
            IOUtils.closeStream(fis);
        }

        if(fos != null){
            IOUtils.closeStream(fdos);
        }
        System.out.println("----------------------------关闭资源完毕-----------");

    }

}
