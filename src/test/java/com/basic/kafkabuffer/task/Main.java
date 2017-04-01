package com.basic.kafkabuffer.task;

import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by 79875 on 2017/4/1.
 */
public class Main {
    @Test
    public void main() throws IOException {
        DataInputFormat dataInputFormat=new DataInputFormat();
        List<InputSplit> splits = dataInputFormat.getSplits("/user/root/wordcount/input/1.log");
        ByteBuffer byteBuffer=ByteBuffer.allocate(1024*1024*128);
        DataInputTask dataInputTask=new DataInputTask(byteBuffer,splits.get(0));
        dataInputTask.run();
        System.out.println("success");
    }
}
