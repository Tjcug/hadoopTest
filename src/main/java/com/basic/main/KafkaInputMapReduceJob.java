package com.basic.main;

import com.basic.model.InputWritable;
import com.basic.util.HdfsOperationUtil;
import com.basic.util.KafkaUtil;
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
 * 提交任务 hadoop 提交任务 hadoop jar hadoopTest-1.0-SNAPSHOT.jar com.basic.main.KafkaInputMapReduceJob /user/root/hadoopkafkainput/input /user/root/hadoopkafkainput/output tweetswordtopic6 60
 */
public class KafkaInputMapReduceJob {
    private Logger logger=Logger.getLogger(KafkaInputMapReduceJob.class);

    public static class KafkaInputMapper extends Mapper<LongWritable, Text, InputWritable, Text> {
        private Logger logger=Logger.getLogger(KafkaInputMapper.class);
        private Configuration configuration;
        private FileSplit inputSplit;
        private long pos=0;
        private long inputDirectorylength;
        private long mapTasks;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logger.info("----------map setup-----------");
            configuration=context.getConfiguration();
            inputSplit = (FileSplit) context.getInputSplit();
            inputDirectorylength = configuration.getLong("inputDirectorylength",0L);
            mapTasks = inputDirectorylength / inputSplit.getLength();
            if(inputDirectorylength % inputSplit.getLength()!=0)
                mapTasks++;
            //logger.info("mapTask-> "+mapTasks+" inputSplitLength->"+inputSplit.getLength());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            pos++;
            long index = (inputSplit.getStart()+10) / 134217728+1;
            //char indexChar=(char) (index+64);
            //str=str+indexChar+pos/mapTasks;
            InputWritable inputWritable=new InputWritable(index,pos);
            //logger.info("key："+key+" mapTask "+mapTasks+" text"+inputWritable.toString());
            context.write(inputWritable,value);
        }
    }

    public static class KafkaInputReducer extends Reducer<InputWritable, Text, Text, Text>{
        private Logger logger=Logger.getLogger(KafkaInputReducer.class);
        private Configuration configuration;
        private KafkaUtil kafkaUtil=KafkaUtil.getInstance();
        private boolean m_bool=true;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            kafkaUtil.init();
            logger.info("----------reduce setup-----------");
            configuration=context.getConfiguration();
        }

        @Override
        protected void reduce(InputWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            //logger.info("---------------reduce------------");
            if(m_bool){
                logger.info("---------------start reduce函数------------");
                m_bool=false;
            }
            while (iterator.hasNext()){
                Text next = iterator.next();
                String topicName=configuration.get("kafkatopic");
            try {
                if(topicName==null||topicName.equals("")){
                    throw new Exception("没有kafkatopic 输入值 请在配置文件中配置");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
                //logger.info("key:"+key+" values"+next.toString());
                kafkaUtil.publishMessage(topicName,key.toString(),next.toString());
                context.write(new Text(key.toString()),next);
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
        job.setMapOutputKeyClass(InputWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(Integer.valueOf(args[3]));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        HdfsOperationUtil hdfsOperationUtil=new HdfsOperationUtil();
        long inputDirectoryLength = hdfsOperationUtil.getInputDirectoryLength(args[0]);
        job.getConfiguration().setLong("inputDirectorylength",inputDirectoryLength);
        job.getConfiguration().set("kafkatopic",args[2]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
