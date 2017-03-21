package com.basic.main;

import com.basic.util.HdfsOperationUtil;

import java.io.IOException;

/**
 * Created by 79875 on 2017/3/18.
 * 提交java运行文件 java -jar hadoopTest-1.0-SNAPSHOT.jar /root/TJ/resultTweets.txt /user/root/input/resultTweets 10485760
 */
public class CopyFileToHDFSBlockMain {

    public static void main(String[] args) throws IOException {
        HdfsOperationUtil operationUtil=new HdfsOperationUtil();
        operationUtil.copyFileToHDFSBlock(args[0],args[1],args[2]);
    }
}
