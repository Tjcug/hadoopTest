package com.basic.hdfsbuffer;

import com.basic.hdfsbuffer.model.HdfsCachePool;
import com.basic.hdfsbuffer.task.DataInputFormat;
import com.basic.hdfsbuffer.task.DataInputTask;
import com.basic.util.KafkaUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by 79875 on 2017/4/1.
 * HDFS文件时序性读取 Version 1.0
 * java -Xmx4028m -Xms4028m -cp hadoopTest-1.0-SNAPSHOT.jar com.basic.hdfsbuffer.Main /user/root/wordcount/input/1.log 10  tweetswordtopic6
 */
public class Main {
    private static final Log LOG = LogFactory.getLog(Main.class);
    private static KafkaUtil kafkaUtil=KafkaUtil.getInstance();
    private static int splitsReaming;//没有处理的数据块剩余多少
    private static int CachePoolBufferNum;//缓冲池缓存Block大小
    private static DataInputFormat dataInputFormat=new DataInputFormat();
    private static List<InputSplit> splits;//输入文件分片的数据类型 InputSplit
    private static long rows=0L;

    public static void main(String[] args) throws Exception {
        long startTimeSystemTime= System.currentTimeMillis();
        splits= dataInputFormat.getSplits(args[0]);
        splitsReaming=splits.size();//数据块剩余多少
        CachePoolBufferNum=Integer.valueOf(args[1]);//缓冲池缓存Block大小
        String kafkatopic=args[2];
        /*
        List<InputSplit> preInputSplits = splits.subList(0, CachePoolBufferNum);
        List<InputSplit> backInputSplits = splits.subList(CachePoolBufferNum, splits.size());
        WheelExecutorDataInputKafka(preInputSplits,backInputSplits);*/
        LinerExecutorDataInputKafka(kafkatopic,splits);
        long endTimeSystemTime = System.currentTimeMillis();
        LOG.info("startTime:"+new Timestamp(startTimeSystemTime));
        LOG.info("endTime:"+new Timestamp(endTimeSystemTime));
        long timelong = (endTimeSystemTime-startTimeSystemTime) / 1000;
        LOG.info("totalTime:"+timelong+" s"+"------or------"+timelong/60+" min");
        System.exit(0);
    }

    public static void runTest() throws Exception {
        String str1="/user/root/wordcount/input/1.log";
        String str2="/user/root/hadoopkafkainput/input/resultTweets.txt";
        splits= dataInputFormat.getSplits(str1);
        splitsReaming=splits.size();//数据块剩余多少
        CachePoolBufferNum=3;//缓冲池缓存Block大小
        /*
        List<InputSplit> preInputSplits = splits.subList(0, CachePoolBufferNum);
        List<InputSplit> backInputSplits = splits.subList(CachePoolBufferNum, splits.size());
        WheelExecutorDataInputKafka(preInputSplits,backInputSplits);
        */
        //LinerExecutorDataInputKafka(splits);
        System.exit(0);
    }

    /**
     * 轮询调度方案
     * @param presplits
     * @param backsplits
     * @throws IOException
     * @throws InterruptedException
     */
    public static void WheelExecutorDataInputKafka(List<InputSplit> presplits,List<InputSplit> backsplits) throws IOException, InterruptedException {
        if(presplits.size()<=0){
            return;//如果剩余数据快大小小于或者等于0 则返回。
        }
        ThreadPoolExecutor executor= new ThreadPoolExecutor(10, 20, 200, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
        HdfsCachePool hdfsCachePool = HdfsCachePool.getInstance(presplits);
        for(int i=0;i<presplits.size();i++){
            DataInputTask dataInputTask=new DataInputTask(hdfsCachePool.getBufferArray()[i],presplits.get(i));
            executor.execute(dataInputTask);
        }

        while (executor.getActiveCount()!=0){
            Thread.sleep(1000);
        }
        for(int j=0;j<presplits.size();j++){
            ByteBuffer byteBuffer = hdfsCachePool.getBufferArray()[j];
            System.out.println(byteBuffer);
            BufferLineReader bufferLineReader=new BufferLineReader(byteBuffer);
            Text text=new Text();
            while (bufferLineReader.readLine(text)!=0){
                //System.out.println(text.toString());
                //System.out.println(byteBuffer);
            }
        }

        //如果剩余的数据快大小大于缓冲池数据块大小
        if(backsplits.size()>CachePoolBufferNum){
            List<InputSplit> preInputSplits = backsplits.subList(0, CachePoolBufferNum);
            List<InputSplit> backInputSplits = backsplits.subList(CachePoolBufferNum, backsplits.size());
            WheelExecutorDataInputKafka(preInputSplits,backInputSplits);
        }
        //如果剩余的数据快大小小于或者等于缓冲池数据块大小
        if(backsplits.size()<=CachePoolBufferNum){
            System.out.println();
            List<InputSplit> preInputSplits = backsplits;
            List<InputSplit> backInputSplits = new ArrayList<>();
            WheelExecutorDataInputKafka(preInputSplits,backInputSplits);
        }
    }

    /**
     * 线性运行调度方案
     */
    public static void LinerExecutorDataInputKafka(String kafkatopic,List<InputSplit> splits) throws IOException, InterruptedException {
        ThreadPoolExecutor executor= new ThreadPoolExecutor(10, 20, 200, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
        HdfsCachePool hdfsCachePool = HdfsCachePool.getInstance(CachePoolBufferNum);

        int j=0,pos=0;
        while (splitsReaming>0 ) {
            if(pos < splits.size() && j<CachePoolBufferNum){
                hdfsCachePool.setInstance(j, splits.get(pos));
                ByteBuffer byteBuffer = hdfsCachePool.getBufferArray()[j];

                DataInputTask dataInputTask = new DataInputTask(byteBuffer, splits.get(pos));
                executor.execute(dataInputTask);
                j++;pos++;
            }

            if(j==CachePoolBufferNum || splitsReaming<=CachePoolBufferNum){
                //j重置为0
                j=0;
                int length=splitsReaming<=CachePoolBufferNum? splitsReaming:CachePoolBufferNum;
                while (executor.getActiveCount()!=0){
                    Thread.sleep(1000);
                }
                for(int num=0;num<length;num++){
                    BufferLineReader bufferLineReader=new BufferLineReader(hdfsCachePool.getBufferArray()[num]);
                    Text text=new Text();
                    System.out.println("-----------------"+ hdfsCachePool.getBufferArray()[num]+"length:"+length +"num:"+num);
                    long startTimeSystemTime= System.currentTimeMillis();
                    while (bufferLineReader.readLine(text)!=0){
                        rows++;
                        kafkaUtil.publishMessage(kafkatopic, String.valueOf(rows),text.toString());
                        //System.out.println(text.toString());
                        //System.out.println(byteBuffer);
                    }
                    long endTimeSystemTime = System.currentTimeMillis();
                    long timelong=(endTimeSystemTime-startTimeSystemTime)/1000;
                    LOG.info("totalTime:"+timelong+" s"+"------or------"+timelong/60+" min");
                    System.out.println("Rows : "+rows);
                    rows=0;
                    splitsReaming--;
                    //kafkaCachePool num缓冲输出到kafka完毕，开始缓存下一块内存块
                    if(splitsReaming>0 && pos<splits.size()){
                        hdfsCachePool.setInstance(num,splits.get(pos));
                        DataInputTask dataInputTaskTmp=new DataInputTask(hdfsCachePool.getBufferArray()[num],splits.get(pos));
                        executor.execute(dataInputTaskTmp);
                        j++;pos++;
                    }
                }
            }
            //如果splitsReaming<=0 则跳出本次循环
            if(splitsReaming<=0){
                break;
            }
        }
    }

}
