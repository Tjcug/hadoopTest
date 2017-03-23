package com.basic.mapper;

import com.basic.reduce.WordCountReduce;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Created by 79875 on 2017/3/21.
 */
public class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Log logger = LogFactory.getLog(WordCountReduce.class);

    static enum Counters { INPUT_WORDS }
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private boolean caseSensitive = true;
    private Set<String> patternsToSkip = new HashSet<String>();
    private long numRecords = 0;
    private String inputFile;

    public void configure(JobConf job) {
        caseSensitive = job.getBoolean("wordcount.case.sensitive", true);
        inputFile = job.get("map.input.file");

        if (job.getBoolean("wordcount.skip.patterns", false)) {
            Path[] patternsFiles = new Path[0];
            try {
                patternsFiles = DistributedCache.getLocalCacheFiles(job);
            } catch (IOException ioe) {
                System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
            }
            for (Path patternsFile : patternsFiles) {
                parseSkipFile(patternsFile);
            }
        }
    }

    private void parseSkipFile(Path patternsFile) {
        try {
            BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
            String pattern = null;
            while ((pattern = fis.readLine()) != null) {
                patternsToSkip.add(pattern);
            }
        } catch (IOException ioe) {
            System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : " + StringUtils.stringifyException(ioe));
        }
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();

        logger.info("-----------------map---------------");
        for (String pattern : patternsToSkip) {
            line = line.replaceAll(pattern, "");
        }

        System.out.println("sout-->map key:" + key.toString());
        logger.info("LOG-->map key:" +  key.toString());
        System.out.println("sout-->map value:" + value.toString());
        logger.info("LOG-->map value:" +  value.toString());

        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            output.collect(word, one);
            System.out.println("sout-->map 分词后 word:" + word.toString());
            logger.info("LOG-->map 分次后 word:" +  word.toString());
            reporter.incrCounter(Counters.INPUT_WORDS, 1);
        }

        if ((++numRecords % 100) == 0) {
            reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
        }
    }
}
