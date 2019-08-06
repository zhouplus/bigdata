package com.zhou.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author changajingzhou
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
        fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), configuration, "root");
    }

    @Test
    public void testMkdir() throws IOException {
        Path path = new Path("/mytemp");
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        fileSystem.mkdirs(path);
    }
    @After
    public void close() throws IOException {

        if (fileSystem != null) {
            fileSystem.close();
        }

    }
}
