package com.basic.hdfsbuffer2;

import com.basic.util.HdfsOperationUtil;
import com.basic.util.KafkaUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;

import java.io.IOException;
import java.sql.Timestamp;

/**
 * locate com.basic.hdfsbuffer
 * Created by 79875 on 2017/4/19.
 * java -Xmx4028m -Xms4028m -cp hadoopTest-1.0-SNAPSHOT.jar com.basic.hdfsbuffer2.KafkaHDFSBenchMark /user/root/flinkwordcount/input/resultTweets.txt testTopic 9
 */
public class KafkaHDFSBenchMark {
    private static final Log LOG = LogFactory.getLog(KafkaHDFSBenchMark.class);
    private static HdfsOperationUtil hdfsOperationUtil=new HdfsOperationUtil();
    private static KafkaUtil kafkaUtil=new KafkaUtil();
    private static byte[] recordDelimiterBytes;//记录分割符
    private static long Totalrows=0L;
    public static void main(String[] args) throws IOException {
        String inputFile=args[0];
        String topic=args[1];
        int partitionsNum=Integer.valueOf(args[2]);
        FileSystem fs = HdfsOperationUtil.getFs();
        FSDataInputStream dataInputStream = fs.open(new Path(inputFile));
        SplitLineReader splitLineReader=new SplitLineReader(dataInputStream,HdfsOperationUtil.getConf(),recordDelimiterBytes);
        long startTimeSystemTime= System.currentTimeMillis();
        Text text=new Text();
        while (splitLineReader.readLine(text)!=0){
            Totalrows++;
            kafkaUtil.publishOrderMessage(topic,partitionsNum,(int)Totalrows,text.toString());
        }
        long endTimeSystemTime = System.currentTimeMillis();
        LOG.info("startTime:"+new Timestamp(startTimeSystemTime));
        LOG.info("endTime:"+new Timestamp(endTimeSystemTime));
        long timelong = (endTimeSystemTime-startTimeSystemTime) / 1000;
        LOG.info("totalTime:"+timelong+" s"+"------or------"+timelong/60+" min");
        System.exit(0);
    }
}
