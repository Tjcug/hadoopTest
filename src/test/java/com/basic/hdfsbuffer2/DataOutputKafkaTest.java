package com.basic.hdfsbuffer2;

import org.junit.Test;

/**
 * locate com.basic.hdfsbuffer2
 * Created by 79875 on 2017/4/11.
 */
public class DataOutputKafkaTest {
    private KafkaDataOutputMain kafkaDataOutput;

    @Test
    public void testKafkaDataOutput() throws Exception {
        kafkaDataOutput =new KafkaDataOutputMain();
        String str1="/user/root/wordcount/input/2.log";
        String str2="/user/root/hadoopkafkainput/input/1.log";
        kafkaDataOutput.TestKafkaDataOutput(str1);
    }

}
