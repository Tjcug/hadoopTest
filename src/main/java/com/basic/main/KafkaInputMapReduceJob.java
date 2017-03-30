package com.basic.main;

import com.basic.util.HdfsOperationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by 79875 on 2017/3/29.
 * 提交任务 hadoop 提交任务 hadoop jar hadoopTest-1.0-SNAPSHOT.jar com.basic.main.KafkaInputMapReduceJob /user/root/wordcount/input /user/root/wordcount/output
 */
public class KafkaInputMapReduceJob {
    private Logger logger=Logger.getLogger(KafkaInputMapReduceJob.class);

    public static class KafkaInputMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Logger logger=Logger.getLogger(KafkaInputMapper.class);
        private Configuration configuration;
        private FileSplit inputSplit;
        private long pos=0;
        private long inputDirectorylength;
        private long mapTasks;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logger.info("----------setup-----------");
            configuration=context.getConfiguration();
            inputDirectorylength = configuration.getLong("inputDirectorylength",0L);
            mapTasks = inputDirectorylength / inputSplit.getLength();
            if(inputDirectorylength % inputSplit.getLength()!=0)
                mapTasks++;
            logger.info("mapTask-> "+mapTasks);
            inputSplit = (FileSplit) context.getInputSplit();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            pos++;
            long index = (inputSplit.getStart()+10) / inputSplit.getLength()+1;
            char indexChar=(char) (index+64);
            String str="";
            str=str+indexChar+pos%mapTasks;
            Text text=new Text(str);
            logger.info("key："+key+" text:"+text.toString());
            context.write(text,value);
        }

    }

    public static class KafkaInputReducer extends Reducer<Text, Text, Text, Text>{
        private Logger logger=Logger.getLogger(KafkaInputReducer.class);
        private Configuration configuration;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            configuration=context.getConfiguration();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            logger.info("---------------reduce------------");
            while (iterator.hasNext()){
                Text next = iterator.next();
                logger.info("key:"+key+" values"+next.toString());
                context.write(key,next);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        Configuration conf = new Configuration();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "kafkaInputMapReduce1");
        job.setJarByClass(KafkaInputMapReduceJob.class);
        job.setMapperClass(KafkaInputMapper.class);
        job.setReducerClass(KafkaInputReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        HdfsOperationUtil hdfsOperationUtil=new HdfsOperationUtil();
        long inputDirectoryLength = hdfsOperationUtil.getInputDirectoryLength(args[0]);
        job.getConfiguration().setLong("inputDirectorylength",inputDirectoryLength);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
