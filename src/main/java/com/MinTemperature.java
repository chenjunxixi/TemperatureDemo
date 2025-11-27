package com;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MinTemperature {
    public static void main(String[] args) {
        Configuration conf = new Configuration();

        // 【关键修改】强制使用 YARN 模式，利用集群算力
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "master");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = null;
        try {
            job = Job.getInstance(conf);
            job.setJarByClass(MinTemperature.class);
            job.setJobName("Min temperature");
            job.setMapperClass(MinTemperatureMapper.class);
            job.setReducerClass(MinTemperatureReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(FloatWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);

            // 输入路径：使用任务 13 清洗后的数据
            FileInputFormat.addInputPath(job,new Path("hdfs://master:9000/china_all/"));
            // 输出路径
            FileOutputFormat.setOutputPath(job,new Path("hdfs://master:9000/output/mintemp/"));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}