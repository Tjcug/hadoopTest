package com.basic.generatedata;

import java.io.*;
import java.sql.Timestamp;

/**
 * Created by 79875 on 2017/4/1.
 * 运行自动生成数据 java -cp hadoopTest-1.0-SNAPSHOT.jar com.basic.generatedata.GenerateData /root/TJ/a.log 5 26
 */
public class GenerateData {

    public static BufferedWriter bufferedWriter=null;
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("input format error! please input true args.");
            System.err.println("For example outputFile rowsByte rows");
            System.exit(2);
        }

        System.out.println("GenerateData init ");
        String outputFile=args[0];
        bufferedWriter=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(outputFile))));
        Integer rowsByte=Integer.valueOf(args[1]);
        Integer rows=Integer.valueOf(args[2]);
        long startTime=System.currentTimeMillis();
        System.out.println("GenerateData startTime:"+new Timestamp(startTime));
        for(int i=0;i<rows;i++){
            String line="";
            int tmp=0;
            while (tmp < rowsByte){
                line+=String.valueOf((char)(i%26+64+1));
                tmp++;
            }
            //System.out.println(line);
            bufferedWriter.write(line);
            bufferedWriter.newLine();
            bufferedWriter.flush();
        }
        bufferedWriter.close();
        long endTime=System.currentTimeMillis();
        System.out.println("GenerateData finish:"+new Timestamp(endTime));
        System.out.println("GenerateData spendTime(s):"+(endTime-startTime)/1000);
        System.out.println("GenerateData line:"+rows+" rowsByte:"+rowsByte+" outputFie"+outputFile);
        System.exit(0);
    }
}
