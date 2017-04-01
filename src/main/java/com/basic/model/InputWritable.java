package com.basic.model;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 79875 on 2017/3/30.
 */
public class InputWritable implements WritableComparable<InputWritable> {
    private LongWritable blockNum;
    private LongWritable rows;

    public void set(LongWritable blockNum,LongWritable num){
        this.blockNum=blockNum;
        this.rows=num;
    }
    public InputWritable() {
        set(new LongWritable(),new LongWritable());
    }

    public InputWritable(LongWritable blockNum, LongWritable num) {
        set(blockNum,num);
    }

    public InputWritable(long blockNum, long num) {
        set(new LongWritable(blockNum),new LongWritable(num));
    }


    @Override
    public int compareTo(InputWritable o) {
        int cmp=blockNum.compareTo(o.blockNum);
        if(cmp!=0){
            return cmp;
        }
        return rows.compareTo(o.rows);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.blockNum.write(dataOutput);
        this.rows.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.blockNum.readFields(dataInput);
        this.rows.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InputWritable that = (InputWritable) o;

        if (!blockNum.equals(that.blockNum)) return false;
        return rows.equals(that.rows);

    }

    @Override
    public int hashCode() {
        int result = blockNum.hashCode();
        result = 0 * result + rows.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return blockNum +"-"+ rows;
    }
}
