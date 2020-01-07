package com.imooc.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.net.URI;

/*
* 使用JAVA API操作HDFS文件系统
* 关键点：
* 1）创建Configuration
* 2) 获取FileSystem
* 3) ...就是你的HDFS API的操作
* */
public class HDFSApp {

    public static void main(String[] args) throws Exception{

        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8020"),configuration,"hadoop");

        Path path = new Path("/rthhdfs/test");
        boolean result = fileSystem.mkdirs(path);
        System.out.println(result);
    }
}
