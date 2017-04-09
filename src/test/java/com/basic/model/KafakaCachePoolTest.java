package com.basic.model;

import com.basic.kafkabuffer.model.KafakaCachePool;
import org.junit.Test;

/**
 * Created by 79875 on 2017/4/1.
 */
public class KafakaCachePoolTest {
    KafakaCachePool kafakaCachePool=KafakaCachePool.getInstance();

    @Test
    public void Test(){
        kafakaCachePool.toString();
    }
}
