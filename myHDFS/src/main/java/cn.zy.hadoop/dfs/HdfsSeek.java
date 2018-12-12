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
 * @Date: 2018/12/4 22:25
 * @Version 1.0
 */
public class HdfsSeek {
    /*
    * 定位读取,如果文件大于块大小，如何只读取其中一块，
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
    }


    @Test//下载第一块
    public void readFileSeek_1() throws IOException {
        //2.获取输入流；
        //从hdfs输入到这里，
        fdis= fs.open(new Path("/user/admin/data/jdk-8u181-linux-x64.tar.gz"));

        //3.获取输出流
        //把从hdfs获取到的数据 输出 到本地，
        fos =new FileOutputStream(new File("d:/mytest/jdk-8u181-linux-x64.tar.gz.part1"));

        //4.流的对拷
        byte[] buf = new byte[1024];//一次读取1kb
        int i;
        for(i = 0;i < 1024 * 128 ; i++){    //块大小为128M
            fdis.read(buf);
            fos.write(buf);
        }

    }


    @Test//下载第2块
    public void readFileSeek_2() throws IOException {
        //2.获取输入流；
        //从hdfs输入到这里，
        fdis= fs.open(new Path("/user/admin/data/jdk-8u181-linux-x64.tar.gz"));

        //3.定位读取位置
        fdis.seek(1024*1024*128);//从哪里开始读取，块大小为128M,故从第一块的末尾开始读，

        //4.获取输出流
        //把从hdfs获取到的数据 输出 到本地，
        fos =new FileOutputStream(new File("d:/mytest/jdk-8u181-linux-x64.tar.gz.part2"));

        //5.流的对拷
        IOUtils.copyBytes(fdis,fos,conf);   //直接读取完剩下的第二块文件

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

    }

}
