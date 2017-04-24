package com.basic.hdfsbuffer2.channelhandler;

import com.basic.hdfsbuffer2.model.HdfsCachePool;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;

/**
 * locate com.basic.hdfsbuffer2.handler
 * Created by 79875 on 2017/4/18.
 */
public class HDFSBufferHandler extends ChannelInboundHandlerAdapter {

    private static final Log LOG = LogFactory.getLog(HDFSBufferHandler.class);
    private HdfsCachePool hdfsCachePool;

    private int blockPosition=0;
    public HdfsCachePool getHdfsCachePool() {
        return hdfsCachePool;
    }

    public void setHdfsCachePool(HdfsCachePool hdfsCachePool) {
        this.hdfsCachePool = hdfsCachePool;
    }

    public HDFSBufferHandler(HdfsCachePool hdfsCachePool) {
        this.hdfsCachePool = hdfsCachePool;
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOG.info("HDFSBufferHandler.channelActive");
       new Thread(new HDFSDataOutputChannelTask(ctx)).start();
//        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER) //flush掉所有写回的数据
//                .addListener(ChannelFutureListener.CLOSE); //当flush完成后关闭channel
//        hdfsCachePool.getChannelFuture().channel().close();
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // 当出现异常就关闭连接
        cause.printStackTrace();
        ctx.close();
    }

    public class HDFSDataOutputChannelTask implements Runnable{
        private ChannelHandlerContext ctx;

        public HDFSDataOutputChannelTask(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            while (true){
                //if(hdfsCachePool.isIsbufferfinish()){
                //可以开始读取HdfsCachePool
                int activeBufferNum = hdfsCachePool.getActiveBufferNum();
                for(int i=0;i<activeBufferNum;i++){
                    while (!hdfsCachePool.isBufferBlockFinished(i)){
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    hdfsCachePool.getBufferArray()[i].setBufferOutFinished(false);
                    ByteBuffer byteBuffer = hdfsCachePool.getBufferArray()[i].byteBuffer;
                    System.out.println("---------start--------"+ byteBuffer +" num:"+i+" blockPosition: "+blockPosition);
                    while (byteBuffer.hasRemaining()){
                        int length=Math.min(4096,byteBuffer.remaining());
                        byte[] buf=new byte[length];
                        byteBuffer.get(buf,0,length);
                        ByteBuf byteBuf=ctx.alloc().buffer(length);
                        byteBuf.writeBytes(buf);
                        ctx.writeAndFlush(byteBuf);
                        //byteBuf.release();
                    }
                    hdfsCachePool.getBufferArray()[i].setBufferOutFinished(true);
                    blockPosition++;
                }
                if(blockPosition>=hdfsCachePool.getInputSplitList().size()){
                    LOG.info("----------------dataOuput over--------------");
                    break;
                }
            }
        }
    }

}
