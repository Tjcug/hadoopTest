package com.basic.main;

import com.basic.mapper.WordCountMapper;
import com.basic.reduce.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by 79875 on 2017/3/21.
 * hdfs dfs -rm -r wordcount/output
 * hadoop 提交任务 hadoop jar hadoopTest-1.0-SNAPSHOT.jar com.basic.main.WorCountMapReduceJob /user/root/wordcount/input /user/root/wordcount/output
 *  两个文件256M*2
 *  	Launched map tasks=20
 *      Launched reduce tasks=20
 *
 * 打开大小写敏感-Dwordcount.case.sensitive=true
 * hadoop jar hadoopTest-1.0-SNAPSHOT.jar com.basic.main.WorCountMapReduceJob -Dwordcount.case.sensitive=true /user/root/wordcount/input /user/root/wordcount/output -skip /user/root/wordcount/patterns.txt
 *              //跳过敏感词汇
 */
public class WorCountMapReduceJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), WordCount.class);
        conf.setJobName(jobname);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(WordCountMapper.class);
        conf.setCombinerClass(WordCountReduce.class);
        conf.setReducerClass(WordCountReduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        List<String> other_args = new ArrayList<String>();
        for (int i=0; i < args.length; ++i) {
            if ("-skip".equals(args[i])) {
                DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
                conf.setBoolean("wordcount.skip.patterns", true);
            } else {
                other_args.add(args[i]);
            }
        }

        FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
        FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

        JobClient.runJob(conf);
        return 0;
    }

    public static final String jobname="wordcount-mapreduce";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }


}
