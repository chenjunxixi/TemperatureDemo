package com;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text,Text, FloatWritable> {
    private static final int MISSING = -9999;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (!"".equals(line)) {
            String[] values = line.split(",");

            if (values.length > 5) {
                String year = values[1];
                String month = values[2];
                String textKey = year + "-" + month;

                try {
                    float temp = Float.parseFloat(values[5]);
                    if (temp != MISSING) {
                        // 【重要】删除了 System.out.println
                        context.write(new Text(textKey), new FloatWritable(temp));
                    }
                } catch (NumberFormatException e) {
                }
            }
        }
    }
}