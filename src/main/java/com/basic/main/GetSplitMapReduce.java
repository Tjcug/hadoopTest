package com.basic.main;

import com.basic.inputformat.RowInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.IOException;
/**
 * Created by 79875 on 2017/3/28.
 * hadoop jar hadoopTest-1.0-SNAPSHOT.jar com.basic.main.GetSplitMapReduce /user/root/wordcount/input /user/root/wordcount/output
 */
public class GetSplitMapReduce {
    private static Logger logger= Logger.getLogger(GetSplitMapReduce.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length!=2){
            System.err.println("Usage databaseV1 <inputpath> <outputpath>");
        }

        Job job = Job.getInstance(conf, GetSplitMapReduce.class.getSimpleName() + "1");
        job.setJarByClass(GetSplitMapReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(MyMapper1.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(RowInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //FileInputFormat.setMaxInputSplitSize(job,1);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);
    }
    public static class MyMapper1 extends Mapper<LongWritable, Text, Text, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
                throws IOException, InterruptedException {

            FileSplit fileSplit=(FileSplit) context.getInputSplit();
            String pathname=fileSplit.getPath().getName();	//获取目录名字
            int depth = fileSplit.getPath().depth();	//获取目录深度
            Class<? extends FileSplit> class1 = fileSplit.getClass(); //获取当前类
            long length = fileSplit.getLength();	//获取文件长度
            SplitLocationInfo[] locationInfo = fileSplit.getLocationInfo();	//获取位置信息
            String[] locations = fileSplit.getLocations();	//获取位置
            long start = fileSplit.getStart(); //The position of the first byte in the file to process.
            String string = fileSplit.toString();
            //fileSplit.
            logger.info("mapper key:"+key.get()+" value:"+value.toString().substring(0,5));
            context.write(new Text("===================================================================================="), NullWritable.get());
            context.write(new Text("pathname--"+pathname), NullWritable.get());
            context.write(new Text("depth--"+depth), NullWritable.get());
            context.write(new Text("class1--"+class1), NullWritable.get());
            context.write(new Text("length--"+length), NullWritable.get());
            context.write(new Text("locationInfo--"+locationInfo), NullWritable.get());
            context.write(new Text("locations--"+locations), NullWritable.get());
            context.write(new Text("start--"+start), NullWritable.get());
            context.write(new Text("string--"+string), NullWritable.get());
        }
    }
}
