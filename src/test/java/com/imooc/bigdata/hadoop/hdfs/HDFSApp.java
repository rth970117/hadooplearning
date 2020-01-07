package com.imooc.bigdata.hadoop.hdfs;

import com.amazonaws.services.dynamodbv2.xspec.S;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sun.nio.ch.IOUtil;

import java.net.URI;

/*
 * 使用JAVA API操作HDFS文件系统
 * 关键点：
 * 1）创建Configuration
 * 2) 获取FileSystem
 * 3) ...就是你的HDFS API的操作
 * */
public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://hadoop000:8020";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws Exception {

        System.out.println("-------setUp------");
        configuration = new Configuration();
        configuration.set("dfs.replication","1");
        /*
         * 构造一个访问指定HDFS系统的客户端对象
         * 第一个参数：HDFS的URI
         * 第二个参数：客户端指定的配置参数
         *  第三个参数：客户端的身份，说白了就是用户名
         * */
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
    }

    @Test
    /*
    * 创建HDFS文件夹
    * */
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/rthhdfs/test"));
    }

    @Test
    /*
    * 查看HDFS内容
    * */
    public void text() throws Exception{
        //FSDataInputStream in = fileSystem.open(new Path("/cdh_version.properties"));
        FSDataInputStream in = fileSystem.open(new Path("/rthhdfs/test/a.txt"));
        IOUtils.copyBytes(in,System.out,1024);
    }

    /*
    * 创建文件
    * */
    @Test
    public void create() throws Exception {
        //FSDataOutputStream out = fileSystem.create(new Path("/rthhdfs/test/a.txt"));
        FSDataOutputStream out = fileSystem.create(new Path("/rthhdfs/test/b.txt"));
        out.writeUTF("hello pk");
        out.flush();
        out.close();

    }

    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
        System.out.println("----tearDown----");
    }

    /*public static void main(String[] args) throws Exception{

        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8020"),configuration,"hadoop");

        Path path = new Path("/rthhdfs/test");
        boolean result = fileSystem.mkdirs(path);
        System.out.println(result);
    }*/
}
