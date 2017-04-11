package com.basic.hdfsbuffer.task;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Test;

import java.util.List;

/**
 * Created by 79875 on 2017/4/1.
 */
public class DataInputFormatTest {
    DataInputFormat dataInputFormat=new DataInputFormat();
    @Test
    public void testGetSplits() throws Exception {
        List<InputSplit> splits = dataInputFormat.getSplits("/user/root/wordcount/input/1.log");
        int i=0;
        for(InputSplit inputSplit:splits){
            FileSplit fileSplit= (FileSplit) inputSplit;
            System.out.println("block"+i+" start:"+fileSplit.getStart()+" length:"+fileSplit.getLength());
            i++;
        }
    }
}
