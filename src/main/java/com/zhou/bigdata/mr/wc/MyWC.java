package com.zhou.bigdata.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author mrchang
 * @description: 单词统计
 * @date 2019-08-14 23:06
 */
public class MyWC {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // Create a new Job
        Job job = Job.getInstance(conf);
        job.setJarByClass(MyWC.class);

        // Specify various job-specific parameters
        job.setJobName("myjob");

        //job.setInputPath(new Path("in"));
        //job.setOutputPath(new Path("out"));

        Path inPath = new Path("/mytemp/helloword.txt");
        FileInputFormat.addInputPath(job,inPath);
        Path outPath = new Path("/output/wordcount");
        //如果输出路径已经有了就先删除
        if(outPath.getFileSystem(conf).exists(outPath)){
            outPath.getFileSystem(conf).delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job,outPath);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }
}
