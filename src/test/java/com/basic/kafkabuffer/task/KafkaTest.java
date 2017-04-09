package com.basic.kafkabuffer.task;

import com.basic.util.KafkaUtil;
import org.junit.Test;

import java.io.IOException;

/**
 * locate ${PACKAGE_NAME}
 * Created by 79875 on 2017/4/8.
 */
public class KafkaTest {

    KafkaUtil kafkaUtil=KafkaUtil.getInstance();

    @Test
    public void producerTest() throws IOException {
        for(int i=0;i<100000000;i++){
            kafkaUtil.publishMessage("tweetswordtopic6",String.valueOf(i),String.valueOf(i));
        }
    }
}
