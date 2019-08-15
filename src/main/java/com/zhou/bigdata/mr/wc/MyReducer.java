package com.zhou.bigdata.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author mrchang
 * @description: 自定义reducer
 * @date 2019-08-15 00:09
 */
public class MyReducer extends Reducer <Text, IntWritable,Text, IntWritable>{

    private IntWritable result = new IntWritable();

    /**
     * 同一组key做一次迭代计算,Key相同的做一次reduce
     *  hello 1
     *  hello 1
     *  hello 1
     *  hello 1
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
