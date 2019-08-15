package com.zhou.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author mrchang
 * @description: 测试HDFS
 * @date 2019-08-07 00:53
 */
public class TestHDFS {

    //放到外面提升作用域
    Configuration configuration = null;
    FileSystem fileSystem = null;

    @Before
    public void connect() throws IOException, URISyntaxException,InterruptedException {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI("hdfs://centos7:8020"), configuration, "root");
    }

    @Test
    public void testMkdir() throws IOException {
        Path path = new Path("/mytemp");
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        fileSystem.mkdirs(path);
    }

    /**
     * 上传文件
     * @param
     * @return
     * @throws
     */
    @Test
    public void uploadFile() throws IOException {

        //文件上传路径
        Path path = new Path("/mytemp/helloword.txt");
        FSDataOutputStream fsDataOutputStream = fileSystem.create(path);

        //拿到磁盘文件
        InputStream is = new BufferedInputStream(new FileInputStream("/Users/changjiangzhou/003_code/003_my/helloworld.txt"));

        //拷贝
        IOUtils.copyBytes(is,fsDataOutputStream,configuration,true);

    }


    /**
     * 读取文件
     * @param
     * @return
     * @throws
     */
    @Test
    public void readFile() throws IOException {

        //文件上传路径
        Path path = new Path("/mytemp/test.txt");
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus,0,fileStatus.getLen());

        //遍历元信息数组
//        for (BlockLocation blk: blockLocations) {
//            //文件块的元信息
//            System.out.println(blk);
//        }

        //读取文件
        FSDataInputStream fdis  = fileSystem.open(path);

        fdis.seek(1048576);


        System.out.println((char) fdis.readByte());
        System.out.println((char) fdis.readByte());
        System.out.println((char) fdis.readByte());
        System.out.println((char) fdis.readByte());
        System.out.println((char) fdis.readByte());
//        System.out.println(fdis.readByte());
//        System.out.println(fdis.readByte());
//        System.out.println(fdis.readByte());
//        System.out.println(fdis.readByte());
//        System.out.println(fdis.readByte());



    }


    @After
    public void close() throws IOException {

        if (fileSystem != null) {
            fileSystem.close();
        }

    }
}
