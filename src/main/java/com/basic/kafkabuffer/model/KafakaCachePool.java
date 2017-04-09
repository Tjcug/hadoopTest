package com.basic.kafkabuffer.model;

import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by 79875 on 2017/4/1.'
 */
public class KafakaCachePool {
    private static KafakaCachePool instance;//缓存池唯一实例

    private ByteBuffer[] bufferArray;

    private int bufferNum=20;

    public KafakaCachePool(List<InputSplit> inputSplitList) throws IOException, InterruptedException {
        init(inputSplitList);
    }

    public KafakaCachePool(int bufferNum) {
        bufferArray=new ByteBuffer[bufferNum];
    }

    public void init(List<InputSplit> inputSplitList) throws IOException, InterruptedException {
//        if(bufferArray!=null)//如果缓冲数组不为空则首先清除缓冲区
//            clearBufferArray();
        bufferArray=new ByteBuffer[inputSplitList.size()];
        for(int i=0; i<bufferArray.length;i++){
            bufferArray[i]=ByteBuffer.allocate((int) inputSplitList.get(i).getLength());//创建一个128M大小的字节缓存区
        }
    }

    /**
     * 得到唯一实例
     * @return
     */
    public synchronized static KafakaCachePool getInstance(){
//        if(instance == null){
//            instance = new KafakaCachePool();
//        }
        return instance;
    }

    public synchronized static KafakaCachePool getInstance(List<InputSplit> inputSplitList) throws IOException, InterruptedException {
        instance = new KafakaCachePool(inputSplitList);
        return instance;
    }

    public synchronized static KafakaCachePool getInstance(int bufferNum) throws IOException, InterruptedException {
        instance = new KafakaCachePool(bufferNum);
        return instance;
    }

    public void setInstance(int bufferindexm,InputSplit inputSplit) throws IOException, InterruptedException {
        bufferArray[bufferindexm]=ByteBuffer.allocate((int) inputSplit.getLength());
    }
    public void datainputBuffer(){

    }

    public ByteBuffer[] getBufferArray() {
        return bufferArray;
    }

    /**
     * 清理缓冲区数组
     */
    public void clearBufferArray(){
        for(int i=0; i<bufferArray.length;i++){
            bufferArray[i].clear();
        }
    }

    public class KafkaCachePoolExecutorTask implements Runnable{

        @Override
        public void run() {

        }
    }
}
