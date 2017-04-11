package com.basic.hdfsbuffer.task;

import com.basic.util.HdfsOperationUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by 79875 on 2017/4/1.
 * Hdfs数据写入到缓存池中
 */
public class DataInputTask implements Runnable {
    private static final Log LOG = LogFactory.getLog(DataInputTask.class);
    private long start;
    private long pos;
    private long end;
    private SplitLineReader in;
    private FSDataInputStream fileIn;
    private Seekable filePosition;
    private boolean isCompressedInput;
    private Decompressor decompressor;
    private byte[] recordDelimiterBytes;

    private ByteBuffer byteBuffer;

    private HdfsOperationUtil hdfsOperationUtil=new HdfsOperationUtil();

    private FileSplit fileSplit;

    private FileSystem fileSystem;//Hdfs文件系统

    private static int block_num=0;

    public DataInputTask(ByteBuffer byteBuffer,InputSplit inputSplit) throws IOException {
        this.byteBuffer = byteBuffer;
        this.fileSplit= (FileSplit) inputSplit;
        fileSystem=HdfsOperationUtil.getFs();
    }

    public void initialize(InputSplit genericSplit) throws IOException {
        FileSplit split = (FileSplit)genericSplit;
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        Path file = split.getPath();
        this.fileIn = fileSystem.open(file);CompressionCodec codec = (new CompressionCodecFactory(HdfsOperationUtil.getConf())).getCodec(file);
        if(null != codec) {
            this.isCompressedInput = true;
            this.decompressor = CodecPool.getDecompressor(codec);
            if(codec instanceof SplittableCompressionCodec) {
                SplitCompressionInputStream cIn = ((SplittableCompressionCodec)codec).createInputStream(this.fileIn, this.decompressor, this.start, this.end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
                this.in = new CompressedSplitLineReader(cIn, HdfsOperationUtil.getConf(), this.recordDelimiterBytes);
                this.start = cIn.getAdjustedStart();
                this.end = cIn.getAdjustedEnd();
                this.filePosition = cIn;
            } else {
                this.in = new SplitLineReader(codec.createInputStream(this.fileIn, this.decompressor), HdfsOperationUtil.getConf(), this.recordDelimiterBytes);
                this.filePosition = this.fileIn;
            }
        } else {
            this.fileIn.seek(this.start);
            this.in = new SplitLineReader(this.fileIn,HdfsOperationUtil.getConf(), this.recordDelimiterBytes);
            this.filePosition = this.fileIn;
        }

//        if(this.start != 0L) {
//            this.start += (long)this.in.readLine(new Text(), 0, this.maxBytesToConsume(this.start));
//        }

        this.pos = this.start;
    }

    @Override
    public void run() {
        try {
            initialize(this.fileSplit);
            //byteBuffer.flip();
            //fileIn.read(byteBuffer);
            byte buf[]=new byte[byteBuffer.capacity()];
            fileIn.readFully(buf,0,byteBuffer.capacity());
            System.out.println("DataInputTask: "+byteBuffer+" block_num"+block_num);
            byteBuffer.put(buf,0,byteBuffer.capacity());
            byteBuffer.clear();
            block_num++;
            System.gc();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}
