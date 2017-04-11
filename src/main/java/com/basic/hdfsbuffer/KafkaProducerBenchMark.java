package com.basic.hdfsbuffer;

import com.basic.util.KafkaUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.sql.Timestamp;

/**
 * locate com.basic.kafkabuffer
 * Created by 79875 on 2017/4/8.
 * 运行函数 java -Xmx4028m -Xms4028m -cp hadoopTest-1.0-SNAPSHOT.jar com.basic.hdfsbuffer.KafkaProducerBenchMark
 */
public class KafkaProducerBenchMark {
    private static final Log LOG = LogFactory.getLog(KafkaProducerBenchMark.class);
    public static KafkaUtil kafkaUtil=KafkaUtil.getInstance();

    public static void main(String[] args) throws IOException {
        long startTimeSystemTime= System.currentTimeMillis();
        for(int i=0;i<3273604;i++){
            String str="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
            kafkaUtil.publishMessage("tweetswordtopic6",String.valueOf(i),str);
        }
        long endTimeSystemTime = System.currentTimeMillis();
        LOG.info("startTime:"+new Timestamp(startTimeSystemTime));
        LOG.info("endTime:"+new Timestamp(endTimeSystemTime));
        long timelong = (endTimeSystemTime-startTimeSystemTime) / 1000;
        LOG.info("totalTime:"+timelong+" s"+"------or------"+timelong/60+" min");
    }
}

/*
2017-04-08 21:20:47  [ kafka-producer-network-thread | producer-1:434 ] - [ DEBUG ]  Initiating connection to node 3 at root10:9092.
2017-04-08 21:20:47  [ kafka-producer-network-thread | producer-1:434 ] - [ DEBUG ]  Initiating connection to node 2 at root9:9092.
2017-04-08 21:20:47  [ kafka-producer-network-thread | producer-1:437 ] - [ DEBUG ]  Completed connection to node 3
2017-04-08 21:20:47  [ kafka-producer-network-thread | producer-1:437 ] - [ DEBUG ]  Initiating connection to node 1 at root8:9092.
2017-04-08 21:20:47  [ kafka-producer-network-thread | producer-1:447 ] - [ DEBUG ]  Completed connection to node 2
2017-04-08 21:20:47  [ kafka-producer-network-thread | producer-1:448 ] - [ DEBUG ]  Completed connection to node 1
2017-04-08 21:20:55  [ main:8229 ] - [ INFO ]  startTime:2017-04-08 21:20:46.797
2017-04-08 21:20:55  [ main:8229 ] - [ INFO ]  endTime:2017-04-08 21:20:55.095
2017-04-08 21:20:55  [ main:8229 ] - [ INFO ]  totalTime:8 s------or------0 min

 */
