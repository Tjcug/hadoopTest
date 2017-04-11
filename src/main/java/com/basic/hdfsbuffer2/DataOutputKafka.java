package com.basic.hdfsbuffer2;

import com.basic.hdfsbuffer2.model.HdfsCachePool;
import com.basic.util.KafkaUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * locate com.basic.hdfsbuffer2
 * Created by 79875 on 2017/4/11.
 * HDFSCachePoll 的数据输出类 数据输出到Kafka中
 */
public class DataOutputKafka {
    private static KafkaUtil kafkaUtil=KafkaUtil.getInstance();
    private static final Log LOG = LogFactory.getLog(DataOutputKafka.class);
    private long Totalrows=0L;
    private long blockrows=0L;
    private HdfsCachePool hdfsCachePool;

    private int blockPosition=0;

    public DataOutputKafka(HdfsCachePool hdfsCachePool) {
        this.hdfsCachePool = hdfsCachePool;
    }

    public void datoutputKafka(String kafkatopic) throws IOException, InterruptedException {
        while (true){
            if(hdfsCachePool.isIsbufferfinish()){
                //可以开始读取HdfsCachePool
                int activeBufferNum = hdfsCachePool.getActiveBufferNum();
                for(int i=0;i<activeBufferNum;i++){
                    BufferLineReader bufferLineReader=new BufferLineReader(hdfsCachePool.getBufferArray()[i]);
                    Text text=new Text();
                    System.out.println("-----------------"+ hdfsCachePool.getBufferArray()[i] +" num:"+i+" blockPosition: "+blockPosition);
                    long startTimeSystemTime= System.currentTimeMillis();
                    while (bufferLineReader.readLine(text)!=0){
                        Totalrows++;
                        blockrows++;
                        kafkaUtil.publishMessage(kafkatopic, String.valueOf(Totalrows),text.toString());
                        //System.out.println(text.toString());
                        //System.out.println(byteBuffer);
                    }
                    System.out.println("BlockRows : "+blockrows);
                    blockPosition++;
                    blockrows=0;
                }
                if(blockPosition>=hdfsCachePool.getInputSplitList().size()){
                    System.out.println("Totalrows : "+Totalrows);
                    LOG.info("----------------dataOuput over--------------");
                    break;
                }
            }
            Thread.sleep(100);
        }
    }
}
