package com.basic.kafkabuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;

import java.io.IOException;

/**
 * Created by 79875 on 2017/4/7.
 */
public class DataReaderInputKafka {
    private static final Log LOG = LogFactory.getLog(DataReaderInputKafka.class);
    public static final String MAX_LINE_LENGTH = "mapreduce.input.linerecordreader.line.maxlength";
    private SplitLineReader in;
    private int maxLineLength;
    private LongWritable key;
    private Text value;
    private boolean isCompressedInput;
    private Decompressor decompressor;
    private byte[] recordDelimiterBytes;

    public DataReaderInputKafka() {
    }

    public DataReaderInputKafka(byte[] recordDelimiter) {
        this.recordDelimiterBytes = recordDelimiter;
    }

    public void initialize() throws IOException {
//        if(null != codec) {
//            this.isCompressedInput = true;
//            this.decompressor = CodecPool.getDecompressor(codec);
//            if(codec instanceof SplittableCompressionCodec) {
//                SplitCompressionInputStream cIn = ((SplittableCompressionCodec)codec).createInputStream(this.fileIn, this.decompressor, this.start, this.end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
//                this.in = new CompressedSplitLineReader(cIn, HdfsOperationUtil.getConf(), this.recordDelimiterBytes);
//                this.start = cIn.getAdjustedStart();
//                this.end = cIn.getAdjustedEnd();
//                this.filePosition = cIn;
//            } else {
//                this.in = new SplitLineReader(codec.createInputStream(this.fileIn, this.decompressor), HdfsOperationUtil.getConf(), this.recordDelimiterBytes);
//                this.filePosition = this.fileIn;
//            }
//        } else {
//            this.fileIn.seek(this.start);
//            this.in = new SplitLineReader(this.fileIn,HdfsOperationUtil.getConf(), this.recordDelimiterBytes);
//            this.filePosition = this.fileIn;
//        }
//
////        if(this.start != 0L) {
////            this.start += (long)this.in.readLine(new Text(), 0, this.maxBytesToConsume(this.start));
////        }
//
//        this.pos = this.start;
    }
}
