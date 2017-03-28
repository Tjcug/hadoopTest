package com.basic.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by 79875 on 2017/3/28.
 */
public class TestMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        System.out.println("========> getPath.getName = " + fileSplit.getPath().getName());
        System.out.println("========> getPath = " + fileSplit.getPath().toString());
        System.out.println("========> getPath.getParent = " + fileSplit.getPath().getParent().toString());
        System.out.println("========> getPath.getParent.getName() = " + fileSplit.getPath().getParent().getName());
    }
}
