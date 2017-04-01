package com.basic.kafkabuffer.task;

import com.basic.util.HdfsOperationUtil;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by 79875 on 2017/4/1.
 */
public class DataInputFormat {

    private HdfsOperationUtil hdfsOperationUtil=new HdfsOperationUtil();
    private Logger LOG = Logger.getLogger(DataInputFormat.class);

    public List<InputSplit> getSplits(String inputpath) throws IOException {

        ArrayList splits = new ArrayList();
        List files = hdfsOperationUtil.listFileStatus(inputpath);
        Iterator i$ = files.iterator();

        while (true) {
            while (true) {
                while (i$.hasNext()) {
                    FileStatus file = (FileStatus) i$.next();
                    Path path = file.getPath();
                    long length = file.getLen();
                    if (length != 0L) {
                        BlockLocation[] blkLocations;
                        if (file instanceof LocatedFileStatus) {
                            blkLocations = ((LocatedFileStatus) file).getBlockLocations();
                        } else {
                            FileSystem blockSize = path.getFileSystem(HdfsOperationUtil.getConf());
                            blkLocations = blockSize.getFileBlockLocations(file, 0L, length);
                        }

                        long blockSize1 = file.getBlockSize();
                        long splitSize = blockSize1;

                        long bytesRemaining;
                        int blkIndex;
                        for (bytesRemaining = length; (double) bytesRemaining / (double) splitSize > 1.1D; bytesRemaining -= splitSize) {
                            blkIndex = this.getBlockIndex(blkLocations, length - bytesRemaining);
                            splits.add(this.makeSplit(path, length - bytesRemaining, splitSize, blkLocations[blkIndex].getHosts(), blkLocations[blkIndex].getCachedHosts()));
                        }

                        if (bytesRemaining != 0L) {
                            blkIndex = this.getBlockIndex(blkLocations, length - bytesRemaining);
                            splits.add(this.makeSplit(path, length - bytesRemaining, bytesRemaining, blkLocations[blkIndex].getHosts(), blkLocations[blkIndex].getCachedHosts()));
                        }

                    } else {
                        splits.add(this.makeSplit(path, 0L, length, new String[0]));
                    }
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Total # of splits generated by getSplits: " + splits.size() + ", TimeTaken: ");
                }

                return splits;
            }
        }
    }

    protected int getBlockIndex(BlockLocation[] blkLocations, long offset) {
        for(int last = 0; last < blkLocations.length; ++last) {
            if(blkLocations[last].getOffset() <= offset && offset < blkLocations[last].getOffset() + blkLocations[last].getLength()) {
                return last;
            }
        }

        BlockLocation var7 = blkLocations[blkLocations.length - 1];
        long fileLength = var7.getOffset() + var7.getLength() - 1L;
        throw new IllegalArgumentException("Offset " + offset + " is outside of file (0.." + fileLength + ")");
    }

    protected FileSplit makeSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts) {
        return new FileSplit(file, start, length, hosts, inMemoryHosts);
    }

    protected FileSplit makeSplit(Path file, long start, long length, String[] hosts) {
        return new FileSplit(file, start, length, hosts);
    }
}

