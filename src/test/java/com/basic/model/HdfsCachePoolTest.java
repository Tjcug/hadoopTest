package com.basic.model;

import com.basic.hdfsbuffer.model.HdfsCachePool;
import org.junit.Test;

/**
 * Created by 79875 on 2017/4/1.
 */
public class HdfsCachePoolTest {
    HdfsCachePool hdfsCachePool = HdfsCachePool.getInstance();

    @Test
    public void Test(){
        hdfsCachePool.toString();
    }
}
