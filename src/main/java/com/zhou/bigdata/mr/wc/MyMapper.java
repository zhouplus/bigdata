package com.zhou.bigdata.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author mrchang
 * @description: 自定义Mapper
 * @date 2019-08-15 00:09
 */
public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    /**
     * @param key     当前行的下标索引
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            // hello 1
            // zhou  1
            // hadoop 1
            context.write(word, one);
        }
    }
}
