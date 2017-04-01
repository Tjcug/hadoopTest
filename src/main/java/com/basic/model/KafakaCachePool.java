package com.basic.model;

import java.nio.ByteBuffer;

/**
 * Created by 79875 on 2017/4/1.'
 */
public class KafakaCachePool {
    private static KafakaCachePool instance;//缓存池唯一实例

    private ByteBuffer[] bufferArray;

    private int bufferNum=20;

    public KafakaCachePool() {
        init();
    }

    public KafakaCachePool(int bufferNum) {
        this.bufferNum = bufferNum;
        init();
    }

    public void init(){
        bufferArray=new ByteBuffer[bufferNum];
        for(ByteBuffer buffer :bufferArray){
            buffer=ByteBuffer.allocate(128*1024*1024);//创建一个128M大小的字节缓存区
        }
    }

    /**
     * 得到唯一实例
     * @return
     */
    public synchronized static KafakaCachePool getInstance(){
        if(instance == null){
            instance = new KafakaCachePool();
        }
        return instance;
    }

    public synchronized static KafakaCachePool getInstance(int bufferNum){
        if(instance == null){
            instance = new KafakaCachePool(bufferNum);
        }
        return instance;
    }

    public void datainputBuffer(){

    }

}
