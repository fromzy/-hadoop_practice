package cn.zy.hadoop.dfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.fs.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @Author: zhang ying
 * @Date: 2018/12/4 13:32
 * @Version 1.0
 */
public class HdfsClient {

    FileSystem fs = null;
    Configuration conf = null;
    boolean flag = false;

    @Before //获取文件系统，
    public  void initHDFS() throws URISyntaxException, IOException, InterruptedException {
        //1.获取文件系统，
        conf = new Configuration();//获取配置信息；
        fs = FileSystem.get(new URI("hdfs://hadoop101:9000"),
                conf,"zy");

        System.out.println("---fs---"+fs.toString());

    }

    @Test//上传本地文件
    public  void  upload() throws IOException {
        //2.上传文件
        conf.set("dfs.replication","2");    //设置副本数，优先级高于配置文件；
        fs.copyFromLocalFile(new Path("d:/mytest/2.txt")
                ,new Path("/user/admin/input"));
    }


    @Test//下载文件
    public  void download()throws IOException{
        //是否删除源文件,源文件路径，是否开启文件校验；
        fs.copyToLocalFile(false
                ,new Path("/user/zy/input/a.txt")
                ,new Path("d:/mytest/aa.txt")/*下载下来的文件需要重命名*/
                ,true);


    }
    @Test//创建目录
    public  void mkdir() throws IOException {
      flag =  fs.mkdirs(new Path("/user/admin/input"));
        //hdfs创建多级目录需要加 -r ,这里不需要；
    }


    @Test//删除文件或目录；
    public  void delete() throws IOException {
        flag =  fs.delete(new Path("/user/admin/input"),true);
        //路径，是否递归删除（true则无论是目录(且目录不为空)还是文件都删除，false则如果是目录(且目录不为空)则异常（删除空目录不报异常））
    }


    @Test//修改文件或者目录的名称
    public void rename() throws IOException {
        flag = fs.rename(new Path("/user/admin/input"),new Path("/user/admin/data"));
        //没有找到路径则返回false,不会抛异常；
    }



    @Test//获取文件详情
    public void readListFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles =
                fs.listFiles(new Path("/"), true);//是否递归；
        while (listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();

            //输出详细信息；
            System.out.println(status.getPath());
            System.out.println(status.getPath().getName());
            System.out.println(status.getLen());//长度
            System.out.println(status.getPermission());//权限
            System.out.println(status.getGroup());//组
            System.out.println(status);

            //获取块信息
            BlockLocation[] bl = status.getBlockLocations();
            for(BlockLocation i : bl){
                String[] hosts = i.getHosts();//获取块的节点
                for(String host : hosts){
                    System.out.println(host);
                }
                System.out.println("--------分割线--------");
            }

        }

    }



    @Test//判断是文件还是文件夹
    public  void  isFile() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus i : fileStatuses){
            if(i.isFile()){
                System.out.println("-----------isfile-----"+i.getPath().getName());
            }else {
                System.out.println("------------------notFile--");
            }
        }
    }

    @After//关闭资源
    public void closeDfs() throws IOException {
        if(fs!=null){
            fs.close();
            System.out.println("over-------------"+flag);
        }
    }

}
