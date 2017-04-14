package com.basic.util;

import org.junit.Test;

/**
 * Created by 79875 on 2017/3/30.
 */
public class KafkaUtilTest {
    KafkaUtil kafkaUtil=KafkaUtil.getInstance();

    @Test
    public void testPublishMessage() throws Exception {
        kafkaUtil.publishOrderMessage("tweetswordtopic5", 6,1,"123");
    }
}
