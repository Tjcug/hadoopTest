package com.basic.reduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by 79875 on 2017/3/21.
 */
public class WordCountReduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {

    private static final Log logger = LogFactory.getLog(WordCountReduce.class);

    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
           }
        System.out.println("Reduce key:"+key.toString() +" count:"+sum);
        logger.info("Reduce key:"+key.toString() +" count:"+sum);
        outputCollector.collect(key, new IntWritable(sum));
        }
    }
